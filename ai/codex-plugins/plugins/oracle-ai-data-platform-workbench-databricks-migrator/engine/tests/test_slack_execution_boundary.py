"""Model-generated and replayed code must not cross the execution boundary live.

Slack neutralisation runs on a cell's ORIGINAL source. Everything that rewrites
the code afterwards -- `call_fix()` model repairs, history replay of stored
`final_code`, auto-repairs, child retries -- previously went straight to
`session.execute()` with no deterministic pass in between.

That matters concretely: neutralising `result = client.files_upload_v2(...)`
leaves `result` undefined, so the next line raises `NameError`. A model asked to
repair that NameError may restore the upload call, and the retry then posts for
real. Prompt instructions are not an execution guard.
"""
from __future__ import annotations

import ast
import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import job_migrate as jm  # noqa: E402

RESTORED_UPLOAD = (
    "from slack_sdk import WebClient\n"
    "client = WebClient(token=tok)\n"
    "result = client.files_upload_v2(channel=ch, file=f)\n"
    "logger.info(result)\n"
)


def _live_calls(source: str) -> list:
    """Calls still executable — comments are invisible to the AST."""
    return [n.func.attr for n in ast.walk(ast.parse(source))
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]


class _FakeSession:
    """Records exactly what would reach the kernel."""

    def __init__(self):
        self.received = []

    async def execute(self, code, **kwargs):
        self.received.append(code)
        return {"status": "ok", "outputs": []}


def test_model_restored_upload_never_reaches_the_session():
    """The reviewer's scenario: call_fix() hands back an active upload."""
    jm._reset_active_slack_bindings()
    session = _FakeSession()

    asyncio.run(jm._safe_execute(session, RESTORED_UPLOAD, tag="test"))

    assert len(session.received) == 1
    delivered = session.received[0]
    assert "files_upload_v2" not in _live_calls(delivered), (
        "a model-restored upload reached the kernel live")
    assert "files_upload_v2" in delivered, "the code must be preserved as a comment"


def test_guard_is_idempotent_on_already_neutralised_code():
    """Code that already went through the cell-loop pass must be untouched, so
    the boundary guard cannot double-comment or re-annotate."""
    jm._reset_active_slack_bindings()
    once = jm._neutralize_for_execution(RESTORED_UPLOAD, tag="t")
    twice = jm._neutralize_for_execution(once, tag="t")
    assert twice == once


def test_guard_leaves_ordinary_code_byte_identical():
    jm._reset_active_slack_bindings()
    code = ("df = spark.read.parquet(p)\n"
            "out = df.groupBy('k').count()\n"
            "producer.send(topic, msg)\n")
    assert jm._neutralize_for_execution(code, tag="t") == code


def test_guard_survives_unparseable_code():
    """A cell that will not parse must still execute, not raise."""
    jm._reset_active_slack_bindings()
    broken = "print 'py2 syntax'\n"
    assert jm._neutralize_for_execution(broken, tag="t") is not None


def test_no_cell_execution_bypasses_the_guard():
    """Structural: a NEW execution site added later must not silently bypass it.

    Every path that runs notebook cell code goes through `_safe_execute`; a
    direct `session.execute(exec_code, ...)` is the shape this guard exists to
    prevent.
    """
    src = (Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py").read_text()
    assert "session.execute(exec_code" not in src, (
        "a cell-execution site bypasses _safe_execute")


def test_every_call_fix_result_is_neutralised_before_being_saved():
    """Not just the executed copy — the SAVED notebook must be safe too.

    Checks EVERY `call_fix` site, not the first one. Written against the first
    only, this test passed while a second site (the %run child path) still
    saved model output unguarded.
    """
    path = Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py"
    src = path.read_text()
    tree = ast.parse(src)

    sites = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        value = node.value.value if isinstance(node.value, ast.Await) else node.value
        if (isinstance(value, ast.Call)
                and getattr(value.func, "id", "") == "call_fix"
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "current_code"):
            sites.append(node.end_lineno)
    assert sites, "no call_fix assignment sites found — did it get renamed?"

    lines = src.splitlines()
    replay = next(n for n in tree.body
                  if isinstance(n, ast.AsyncFunctionDef)
                  and n.name == "_replay_cell_entry")
    replay_src = "\n".join(lines[replay.lineno - 1:replay.end_lineno])
    # Replay checks the changed durable source at the start of the next loop
    # attempt. This covers injected fixed_code on attempt 0 as well as call_fix
    # output on retries, before either execution or save-back.
    replay_guarded = (
        "_check_replay_source(original_code, current_code" in replay_src
        and replay_src.index("_check_replay_source(original_code, current_code")
        < replay_src.index("exec_code = _prepare_cell_redirects")
        and replay_src.index("source_changed = True", replay_src.index("current_code = await call_fix"))
        < replay_src.index("_persist_replayed_source(entry, current_code)")
    )
    unguarded = [
        ln for ln in sites
        if "_neutralize_for_execution(" not in "\n".join(lines[ln: ln + 14])
        and not (replay.lineno <= ln <= replay.end_lineno and replay_guarded)
    ]
    assert not unguarded, (
        f"call_fix output saved without neutralisation at line(s): {unguarded}")


# ═══════════════════════════════════════════════════════════════════════════
# N1(c) / N2 / N4: fail-closed guard, model output, child ordering.
# ═══════════════════════════════════════════════════════════════════════════

def test_guard_failure_aborts_instead_of_returning_the_original():
    """An internal guard error used to return the ORIGINAL source, which is the
    fail-OPEN direction: an unanalysed cell reaches the kernel intact."""
    jm._reset_active_slack_bindings()
    boom = object()                      # not a str -> forces an internal error

    class _Explodes:
        def __getattr__(self, _):
            raise RuntimeError("analyser exploded")

    original = jm._ast_neutralize_cell
    jm._ast_neutralize_cell = lambda *a, **k: _Explodes()
    try:
        with pytest.raises(jm.SlackGuardError):
            jm._neutralize_for_execution(RESTORED_UPLOAD, tag="t")
    finally:
        jm._ast_neutralize_cell = original


def test_unresolvable_cell_is_refused_not_executed():
    """When even the line-level fail-closed pass cannot clear the evidence, the
    cell must be refused rather than executed."""
    jm._reset_active_slack_bindings()

    class _Unsafe:
        source = RESTORED_UPLOAD
        commented = 0
        neutralised = []
        unprovable = []
        parse_error = "SyntaxError: x"
        unsafe = True

    original = jm._ast_neutralize_cell
    jm._ast_neutralize_cell = lambda *a, **k: _Unsafe()
    try:
        session = _FakeSession()
        with pytest.raises(jm.SlackGuardError):
            asyncio.run(jm._safe_execute(session, RESTORED_UPLOAD, tag="t"))
        assert session.received == [], "an uncertifiable cell reached the kernel"
    finally:
        jm._ast_neutralize_cell = original


def test_history_replay_goes_through_the_guard():
    """`executed_code` was replayed with a direct session.execute() after a
    kernel reconnect, so a stored live upload ran again mid-migration."""
    src = (Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py").read_text()
    assert "session.execute(prev_code" not in src, (
        "history replay bypasses _safe_execute")


def test_model_output_is_neutralised_before_it_is_persisted():
    """CELL_MIGRATE_PROMPT output went straight into the saved notebook. In
    code-only mode (dependencies) it is never executed, so the execution-time
    guard would never see it at all."""
    src = (Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py").read_text()
    anchor = 'migrated_cell["source"] = [migrated_code]'
    positions = [i for i in range(len(src))
                 if src.startswith(anchor, i)]
    assert positions, "persistence site not found — did it get renamed?"
    for pos in positions:
        window = src[max(0, pos - 1500): pos]
        # One site persists a tool-authored `%run <path>` literal, which cannot
        # contain a customer Slack call; every other site must be guarded.
        if "migrated_code = f'%run {child_path}'" in window:
            continue
        assert ("_neutralize_for_execution(" in window
                or "_finalize_migrated_cell(" in window), (
            f"migrated_code persisted unguarded near offset {pos}")
    # Normal execution and stopped validation share this finalizer. Keep the
    # safety boundary pinned even if save sites move farther from the guard.
    import inspect
    finalizer = inspect.getsource(jm._finalize_migrated_cell)
    assert "_neutralize_for_execution(" in finalizer


def test_no_notebook_code_variable_bypasses_the_guard():
    """Stronger than a single-string search: any `session.execute(<var>)` whose
    variable is not a known bootstrap is a potential bypass."""
    path = Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py"
    tree = ast.parse(path.read_text())

    # Bootstraps and generated setup snippets are tool-authored, not notebook
    # code, and cannot contain customer Slack calls.
    # Tool-AUTHORED snippets: bootstraps, probes and generated helpers. They
    # are built from f-strings in this file, never from notebook source, so
    # they cannot carry a customer Slack call. Everything else must go through
    # _safe_execute. Adding a name here is a deliberate assertion that the
    # variable never holds notebook code.
    ALLOWED = {
        "bootstrap", "params_code", "code", "setup", "_setp", "probe", "_probe",
        "src", "_code", "_bs", "_wrapper", "write_mp_code",
        "_sibling_copy_code", "_mp_snapshot_probe", "_mp_restore_probe",
        "_AIDPUTILS_WIDGET_SEED_CODE",
        # redirect-schema existence re-verify (cold-metastore false-negative
        # hardening, 2026-09-10): pure f-string over schema_name — never
        # notebook code.
        "exists_probe",
    }
    # _safe_execute now neutralizes before checking kernel-global collisions.
    # Permit its own direct submission only when the last assignment to the
    # submitted variable is the neutralizer. Never whitelist that variable at
    # other call sites (where it could hold unguarded notebook source).
    boundary = next(n for n in tree.body
                    if isinstance(n, ast.AsyncFunctionDef) and n.name == "_safe_execute")
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        if not (isinstance(fn, ast.Attribute) and fn.attr == "execute"):
            continue
        if not (isinstance(fn.value, ast.Name) and fn.value.id == "session"):
            continue
        if not node.args:
            continue
        first = node.args[0]
        if (isinstance(first, ast.Name) and first.id == "canonical"
                and boundary.lineno <= node.lineno <= boundary.end_lineno):
            assignments = [n for n in ast.walk(boundary)
                           if isinstance(n, ast.Assign) and n.lineno < node.lineno
                           and any(isinstance(t, ast.Name) and t.id == first.id
                                   for t in n.targets)]
            assert assignments, "execution boundary lost its guarded assignment"
            last = max(assignments, key=lambda n: n.lineno)
            assert (isinstance(last.value, ast.Call)
                    and getattr(last.value.func, "id", None) == "_neutralize_for_execution")
            continue
        if isinstance(first, ast.Name) and first.id not in ALLOWED:
            offenders.append((node.lineno, first.id))
    assert not offenders, f"unguarded session.execute(<notebook code>): {offenders}"


def test_child_cells_are_neutralised_just_in_time_not_pre_scanned():
    """Pre-scanning the whole child advanced bindings through its LAST cell
    before the FIRST one ran, handing an earlier nested %run future state."""
    path = Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py"
    tree = ast.parse(path.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
              and n.name == "_inline_child_notebook")

    calls = [n.lineno for n in ast.walk(fn) if isinstance(n, ast.Call)
             and getattr(n.func, "id", "") == "_neutralize_child_cell"]
    loops = [n.lineno for n in ast.walk(fn) if isinstance(n, ast.For)
             and isinstance(n.target, ast.Tuple)
             and any(getattr(e, "id", "") == "child_source" for e in n.target.elts)]
    assert calls and loops, "child neutralisation or execution loop not found"
    assert min(calls) > min(loops), (
        "child cells are neutralised before the execution loop — that is the "
        "pre-scan ordering bug")


def test_parallel_dependencies_do_not_share_binding_state():
    """Sibling dependencies migrate concurrently through asyncio.gather().

    With process-global binding state, dependency A binds
    `client = WebhookClient(...)`, yields, dependency B resets the global, and
    A's next guarded cell is judged against B's empty state — so a
    model-produced `client.send(message)` was delivered LIVE, decided purely by
    coroutine scheduling. The state is task-local now, and call sites that have
    the notebook's own Bindings pass them explicitly.
    """
    async def scenario(pass_explicitly):
        async def dep_a():
            own = jm._reset_active_slack_bindings()
            jm._update_slack_bindings(
                "from slack_sdk.webhook import WebhookClient\n"
                "client = WebhookClient(url=u)\n", own)
            await asyncio.sleep(0.01)                 # yield to the sibling
            session = _FakeSession()
            kwargs = {"bindings": own} if pass_explicitly else {}
            await jm._safe_execute(session, "client.send(message)\n",
                                   tag="dep-a", **kwargs)
            return session.received[0]

        async def dep_b():
            jm._reset_active_slack_bindings()          # sibling resets
            await asyncio.sleep(0.02)

        delivered, _ = await asyncio.gather(dep_a(), dep_b())
        return delivered

    for explicit in (False, True):
        delivered = asyncio.run(scenario(explicit))
        assert "send" not in _live_calls(delivered), (
            f"parallel dependency leaked a live send (explicit={explicit})")


def test_binding_state_is_not_a_module_global():
    """A plain module global cannot be safe here — asyncio.gather() siblings
    would share it. Pinned so it is not reintroduced."""
    src = (Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py").read_text()
    assert "_SLACK_BINDINGS_VAR" in src
    assert "global _ACTIVE_SLACK_BINDINGS" not in src


# ═══════════════════════════════════════════════════════════════════════════
# G1: the unparseable-cell fallback must consult notebook-wide bindings and
# recognise every callable spelling, not just `.method(`.
# ═══════════════════════════════════════════════════════════════════════════

PREV_CELL_BINDS_CLIENT = ("from slack_sdk.webhook import WebhookClient\n"
                          "client = WebhookClient(url=url)\n")


@pytest.mark.parametrize("cell,label", [
    ('files = !echo x\nclient.send(text="POSTED")\n', "cross-cell binding"),
    ('files = !echo x\n(client.send)(text="POSTED")\n', "parenthesised callable"),
    ('files = !echo x\npost = client.send\npost(text="POSTED")\n',
     "bound method assigned then called"),
])
def test_unparseable_cell_respects_cross_cell_bindings(cell, label):
    """The shell assignment makes these invalid for `ast.parse` but valid
    IPython. The fallback judged Slack evidence from the CURRENT cell's text
    only and matched only the literal `.method(` spelling, so a client bound in
    an earlier cell was invisible and all three were certified safe.
    """
    jm._reset_active_slack_bindings()
    state = jm._new_slack_bindings()
    jm._update_slack_bindings(PREV_CELL_BINDS_CLIENT, state)
    assert state.get("client") is True

    session = _FakeSession()
    asyncio.run(jm._safe_execute(session, cell, tag="t", bindings=state))

    assert len(session.received) == 1
    delivered = session.received[0]
    assert delivered != cell, f"{label}: guard returned the source unchanged"
    assert "# " in delivered, f"{label}: nothing was commented"


def test_unparseable_fallback_leaves_unrelated_code_alone():
    """No Slack bindings and no evidence — nothing should be touched."""
    jm._reset_active_slack_bindings()
    for cell in ("print 'py2'\nproducer.send(topic, m)\n",
                 "print 'py2'\ndf.count()\n"):
        session = _FakeSession()
        asyncio.run(jm._safe_execute(session, cell, tag="t"))
        assert session.received[0] == cell


def test_unparseable_fallback_keeps_reads_running():
    """A read on a bound client is still a read."""
    state = jm._new_slack_bindings()
    jm._update_slack_bindings(PREV_CELL_BINDS_CLIENT, state)
    cell = 'files = !echo x\nr = client.conversations_history(channel="C1")\n'
    session = _FakeSession()
    asyncio.run(jm._safe_execute(session, cell, tag="t", bindings=state))
    assert session.received[0] == cell
