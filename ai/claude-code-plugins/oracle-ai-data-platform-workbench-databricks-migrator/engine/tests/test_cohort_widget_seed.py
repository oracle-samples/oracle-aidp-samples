"""Cohort / notebook.run inline path must seed the NATIVE aidputils widget store.

Incident (2026-09-24, Example_Txn_Budget_Monitoring run cccc8f6d): a cohort child
invoked via notebook.run read `aidputils.widgets.get("experiment_name")` and raised
"Widget not found". The cohort inline path wrote the param to legacy Spark configuration and
manifest_params.json (which backed old Scala parameter reads and aidp_compat
_AIDPParams.getParameter) but NOT the native widget store that aidputils.widgets.get
reads. Regression: the dbutils->aidputils widgets mapping seeded only the top-level
task path (_AIDPUTILS_WIDGET_SEED_CODE), not the cohort path. These tests pin the
deterministic fix (_aidputils_widget_seed_setp) and that BOTH cohort inject sites
call it.
"""
from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import job_migrate as jm  # noqa: E402


def test_seed_setp_emits_native_widget_text_for_each_param():
    code = jm._aidputils_widget_seed_setp({"experiment_name": "M14"})
    # The Python seed populates the shared native Python/Scala widget store.
    assert "if 'aidputils' in dir():" in code
    assert "aidputils.widgets.text(" in code
    assert "experiment_name" in code and "M14" in code
    # Absent-key remove stays guarded (idempotent upsert); the correctness-critical
    # set/clear now FAIL-CLOSED (propagate) — see the failure-path tests below.
    assert "try:" in code and "except Exception:" in code


def test_seed_setp_is_valid_python_and_seeds_a_fake_store():
    # The emitted snippet must run cleanly on a kernel and populate a fake
    # aidputils.widgets store. The reset is now FAIL-CLOSED on a missing baseline
    # snapshot, so a valid run must provide `_wid_orig` (the pre-inline baseline).
    code = jm._aidputils_widget_seed_setp({"experiment_name": "M14", "n": None})

    class _W:
        def __init__(self): self.store = {}
        def getAll(self): return dict(self.store)
        def remove(self, k): self.store.pop(k, None)
        def text(self, k, v): self.store[k] = v

    class _A:
        widgets = _W()

    ns = {"aidputils": _A(), "_wid_orig": {}}
    exec(code, ns)
    assert ns["aidputils"].widgets.store == {"experiment_name": "M14", "n": ""}


def test_seed_setp_empty_params_still_resets_but_does_not_seed():
    """PR#150 P1 round 3: a child with no/unresolved params must STILL reset the
    shared store (so it can't inherit a prior child's keys) — it just seeds nothing.
    An early `return ''` on empty params would skip the reset and leak."""
    for empty in ({}, None):
        code = jm._aidputils_widget_seed_setp(empty)
        assert "aidputils.widgets.remove(_wk)" in code and "_wid_orig" in code, "empty params must still reset"
        assert "for _wk, _wv in {" not in code, "empty params must not seed"

    # exec the empty-param reset against a store holding a prior child's key
    class _W:
        def __init__(self, init): self.store = dict(init)
        def getAll(self): return dict(self.store)
        def remove(self, k): self.store.pop(k, None)
        def text(self, k, v): self.store[k] = v
    class _A:
        def __init__(self, init): self.widgets = _W(init)
    a = _A({"job": "J", "leaked_from_prev": "X"})
    ns = {"aidputils": a, "_wid_orig": {"job": "J"}}
    exec(jm._aidputils_widget_seed_setp({}), ns)   # param-less child
    assert a.widgets.store == {"job": "J"}          # prior child's key removed, baseline kept


def test_seed_setp_resets_baseline_then_seeds_isolating_prior_child():
    """Per-child isolation (PR#150 P1 round 2): children share the kernel, so the
    seed must first reset the store to the pre-inline baseline (_wid_orig) — dropping
    a PRIOR child's keys and restoring master's originals — then seed THIS child.
    So child B never inherits child A's value for a key B was not given."""
    class _W:
        def __init__(self, init): self.store = dict(init)
        def getAll(self): return dict(self.store)
        def remove(self, k): self.store.pop(k, None)
        def text(self, k, v): self.store[k] = v
    class _A:
        def __init__(self, init): self.widgets = _W(init)

    baseline = {"job_param": "J"}                       # master's original widgets
    # store currently holds baseline + a PRIOR child's leaked keys
    a = _A({"job_param": "J", "experiment_name": "M14_prev", "extra_from_A": "X"})
    ns = {"aidputils": a, "_wid_orig": dict(baseline)}
    # seed THIS child (B) with only experiment_name
    exec(jm._aidputils_widget_seed_setp({"experiment_name": "Txn26"}), ns)
    # prior child's key (extra_from_A) removed; experiment_name is B's value; baseline intact
    assert a.widgets.store == {"job_param": "J", "experiment_name": "Txn26"}


def test_seed_setp_per_child_reset_is_present_in_source():
    src = jm._aidputils_widget_seed_setp({"k": "v"})
    # FAIL-CLOSED (PR#150 review r4): a missing baseline snapshot must RAISE (so the
    # caller skips the child), not silently skip the reset (the old fail-open).
    assert "'_wid_orig' not in dir()" in src and "raise RuntimeError(" in src, \
        "per-child reset must FAIL-CLOSED when the baseline snapshot is unavailable"
    assert "aidputils.widgets.remove(_wk)" in src, "per-child reset must drop a prior child's keys"


class _PreserveWidgets:
    """Mirrors the NATIVE aidputils contract verified on cluster c91dc735 (2026-09-24):
    text() PRESERVES an existing value (sets only when the key is absent). remove()
    deletes; get() raises when absent."""
    def __init__(self, init=None): self.store = dict(init or {})
    def getAll(self): return dict(self.store)
    def remove(self, k): self.store.pop(k, None)
    def text(self, k, v):
        if k not in self.store:           # PRESERVE (native contract)
            self.store[k] = v
    def get(self, k):
        if k not in self.store:
            raise Exception("widget not found: " + k)
        return self.store[k]


class _PreserveAidp:
    def __init__(self, init=None): self.widgets = _PreserveWidgets(init)


def test_widget_seed_child_value_wins_over_baseline_with_preserve_text():
    """PR#150 review (o-vazunnavar): native text() PRESERVES an existing value, so when
    a child's param name COLLIDES with a parent master widget (a baseline key), the
    seed must remove-then-text or the child silently reads the parent's value. Uses the
    PRESERVE-semantics fake (the real contract) — the old overwrite-fake hid this."""
    a = _PreserveAidp({"job": "J", "experiment_name": "PARENT"})       # baseline has the key
    ns = {"aidputils": a, "_wid_orig": {"job": "J", "experiment_name": "PARENT"}}
    exec(jm._aidputils_widget_seed_setp({"experiment_name": "M14"}), ns)   # child collides
    assert a.widgets.store["experiment_name"] == "M14", \
        "child value must override the parent's baseline value (remove-then-text)"
    assert a.widgets.store["job"] == "J"                                   # untouched baseline preserved


def test_widget_empty_child_resets_collision_key_to_baseline_with_preserve_text():
    """Between-child isolation on a collision key under PRESERVE semantics: child A sets
    experiment_name=M14; param-less child B must see the BASELINE value (PARENT), not
    A's M14. Requires the reset baseline-restore to remove-then-text."""
    a = _PreserveAidp({"job": "J", "experiment_name": "PARENT"})
    ns = {"aidputils": a, "_wid_orig": {"job": "J", "experiment_name": "PARENT"}}
    exec(jm._aidputils_widget_seed_setp({"experiment_name": "M14"}), ns)   # child A
    assert a.widgets.store["experiment_name"] == "M14"
    exec(jm._aidputils_widget_seed_setp({}), ns)                          # param-less child B
    assert a.widgets.store["experiment_name"] == "PARENT", \
        "empty child B must read the baseline value, not child A's leaked value"


def test_both_cohort_inject_sites_seed_the_widget_store():
    """Regression guard: the direct notebook.run AND the cohort fan-out inject
    sites must both call the widget seed — otherwise a cohort child's
    aidputils.widgets.get raises 'Widget not found' (the incident)."""
    src = (_SCRIPTS / "job_migrate.py").read_text(encoding="utf-8")
    assert src.count("_setp += _aidputils_widget_seed_setp(_mp_updates)") >= 2, (
        "both cohort/notebook.run param-inject sites must seed the native widget store"
    )


def test_manifest_params_perchild_reset_isolates_prior_child(tmp_path):
    """getParameter-path parity: manifest_params.json is reset to the master
    baseline (_mp_orig_content) then merged with THIS child's params — for every
    child, even param-less — so a prior child's params don't leak. Exec the emitted
    code against a file a prior child already polluted."""
    import json
    mp = tmp_path / "manifest_params.json"
    baseline = json.dumps({"job": "J"})
    # child WITH params: prior child's 'leaked' key dropped, baseline + this child kept
    mp.write_text(json.dumps({"job": "J", "leaked": "A"}))
    exec(jm._manifest_params_perchild_setp(str(mp), {"experiment_name": "M14"}),
         {"_mp_orig_content": baseline})
    assert json.loads(mp.read_text()) == {"job": "J", "experiment_name": "M14"}
    # param-less child: resets to baseline only (prior child's 'leaked' dropped)
    mp.write_text(json.dumps({"job": "J", "leaked": "A"}))
    exec(jm._manifest_params_perchild_setp(str(mp), {}), {"_mp_orig_content": baseline})
    assert json.loads(mp.read_text()) == {"job": "J"}


def test_both_cohort_sites_reset_manifest_params_per_child():
    src = (_SCRIPTS / "job_migrate.py").read_text(encoding="utf-8")
    assert src.count("_setp += _manifest_params_perchild_setp(_mp_path, _mp_updates)") >= 2, (
        "both cohort/notebook.run sites must reset manifest_params per child"
    )


_UNSET = object()


class _FakeConf:
    """Minimal spark.conf (RuntimeConfig) stand-in. Models the real 2-arg contract:
    get(k) raises when unset; get(k, default) returns the default when unset (no raise).
    The isolation code uses the 2-arg form so an absent key is distinguished from a
    genuine read error (review r4 #3)."""
    def __init__(self, init=None): self.store = dict(init or {})
    def get(self, k, default=_UNSET):
        if k in self.store:
            return self.store[k]
        if default is not _UNSET:
            return default
        raise Exception(f"Cannot find config {k}")
    def set(self, k, v): self.store[k] = v
    def unset(self, k): self.store.pop(k, None)


class _FakeSpark:
    def __init__(self, init=None): self.conf = _FakeConf(init)


def test_native_scala_widget_scope_captures_baseline_and_sets_child():
    a = _PreserveAidp({'job': 'J'})
    ns = {'aidputils': a, '_wid_orig': dict(a.widgets.store)}
    exec(jm._aidputils_widget_seed_setp({'experiment_name': 'M14', 'n': None}), ns)
    assert a.widgets.store == {'job': 'J', 'experiment_name': 'M14', 'n': ''}
    assert ns['_wid_orig'] == {'job': 'J'}


def test_native_scala_widget_scope_isolates_prior_child():
    a = _PreserveAidp({'job': 'J'})
    ns = {'aidputils': a, '_wid_orig': {'job': 'J'}}
    exec(jm._aidputils_widget_seed_setp({'experiment_name': 'M14', 'region': 'us'}), ns)
    exec(jm._aidputils_widget_seed_setp({'experiment_name': 'Txn26'}), ns)
    assert a.widgets.store == {'job': 'J', 'experiment_name': 'Txn26'}


def test_native_scala_paramless_child_still_resets():
    a = _PreserveAidp()
    ns = {'aidputils': a, '_wid_orig': {}}
    exec(jm._aidputils_widget_seed_setp({'experiment_name': 'M14'}), ns)
    assert a.widgets.get('experiment_name') == 'M14'
    exec(jm._aidputils_widget_seed_setp({}), ns)
    assert 'experiment_name' not in a.widgets.store




def test_native_scala_widget_valid_unset_marker_value_not_dropped():
    a = _PreserveAidp({'experiment_name': '__AIDP_UNSET__'})
    ns = {'aidputils': a, '_wid_orig': dict(a.widgets.store)}
    exec(jm._aidputils_widget_seed_setp({'experiment_name': 'M14'}), ns)
    assert a.widgets.get('experiment_name') == 'M14'
    exec(jm._aidputils_widget_seed_setp({}), ns)
    assert a.widgets.get('experiment_name') == '__AIDP_UNSET__'


def test_native_scala_full_loop_unset_baseline_two_child_then_parent():
    import pytest
    a = _PreserveAidp()
    ns = {'aidputils': a, '_wid_orig': {}}
    exec(jm._aidputils_widget_seed_setp({'experiment_name': 'M14'}), ns)
    assert a.widgets.get('experiment_name') == 'M14'
    exec(jm._aidputils_widget_seed_setp({}), ns)
    with pytest.raises(Exception, match='widget not found'):
        a.widgets.get('experiment_name')
    exec(jm._aidputils_widget_seed_setp({}), ns)
    with pytest.raises(Exception, match='widget not found'):
        a.widgets.get('experiment_name')


def test_native_scala_full_loop_restores_parent_baseline_value():
    a = _PreserveAidp({'experiment_name': 'PARENT'})
    ns = {'aidputils': a, '_wid_orig': dict(a.widgets.store)}
    exec(jm._aidputils_widget_seed_setp({'experiment_name': 'M14'}), ns)
    assert a.widgets.get('experiment_name') == 'M14'
    exec(jm._aidputils_widget_seed_setp({}), ns)
    assert a.widgets.get('experiment_name') == 'PARENT'
    exec(jm._aidputils_widget_seed_setp({}), ns)
    assert a.widgets.get('experiment_name') == 'PARENT'


def test_both_cohort_sites_isolate_native_widgets_per_child():
    src = (_SCRIPTS/'job_migrate.py').read_text()
    assert src.count('_setp += _aidputils_widget_seed_setp(_mp_updates)') >= 2
    assert '_spark_conf_perchild_setp' not in src
    assert 'spark.aidp.param.' not in src


def test_native_widget_scope_snapshotted_and_restored_around_cohort_inline():
    src = (_SCRIPTS/'job_migrate.py').read_text()
    assert '_wid_orig = dict(aidputils.widgets.getAll())' in src
    assert 'for _k, _v in _wid_orig.items()' in src
    assert '_spark_orig' not in src


def test_orchestration_notebook_run_two_child_nonempty_then_empty(monkeypatch, tmp_path):
    """Preserve PR#150's orchestration pin with the shared native widget store."""
    from test_child_exit_conversion import _run
    notebooks = {
        'childA': ['a = dbutils.widgets.get("experiment_name")', 'print("A", a)'],
        'childB': ['b = dbutils.widgets.get("experiment_name")', 'print("B", b)'],
    }
    parent = ['x = 1',
        'dbutils.notebook.run("/Workspace/childA", 3600, {"experiment_name": "M14"})',
        'dbutils.notebook.run("/Workspace/childB", 3600, {})']
    result, saved, kernel, installs, logs = _run(monkeypatch, tmp_path, notebooks, parent=parent)
    executed = '\n'.join(kernel.executed)
    assert '_wid_orig = dict(aidputils.widgets.getAll())' in executed
    assert executed.count("for _wk, _wv in {'experiment_name': 'M14'}.items()") == 1
    assert executed.count("if '_wid_orig' not in dir() or _wid_orig is None:") >= 2
    assert 'spark.aidp.param.' not in executed
    assert 'for _k, _v in _wid_orig.items()' in executed


def test_widget_store_is_snapshotted_and_restored_around_cohort_inline():
    """The seed mutates the SHARED kernel widget store, so the cohort inline block
    must snapshot it before and restore it after (B1 parity with manifest_params) —
    otherwise a later master cell / next child reads a prior child's value via
    aidputils.widgets.get (silent-wrong leak). Pin both halves."""
    src = (_SCRIPTS / "job_migrate.py").read_text(encoding="utf-8")
    # snapshot before the loop
    assert "_wid_orig = dict(aidputils.widgets.getAll())" in src, "widget store not snapshotted"
    # restore after the loop: remove seeded keys + restore originals
    assert "aidputils.widgets.remove(_k)" in src, "seeded widget keys not removed on restore"
    assert "for _k, _v in _wid_orig.items()" in src, "original widget values not restored"


# ── FAIL-CLOSED isolation (PR#150 review round-4, o-vazunnavar) ───────────────────
# Parameter isolation is correctness-critical setup: if the reset/seed of the widget
# or manifest store cannot be established, the child MUST NOT run — it would read a
# prior child's value (a reachable false-PASS). The critical clear/set ops therefore
# propagate their failure; `_isolation_guarded` turns that into a `_ISO_FAIL` marker
# (absence of `_ISO_OK`) the caller acts on. These pin the exact deterministic probes
# from the review plus a positive control.
import io as _io
import contextlib as _ctx


def _run_guarded(setp_code, ns):
    """exec the `_isolation_guarded`-wrapped setp against a copy of ns; return stdout."""
    buf = _io.StringIO()
    with _ctx.redirect_stdout(buf):
        exec(jm._isolation_guarded(setp_code), dict(ns))
    return buf.getvalue()


class _RaiseRemoveWidgets(_PreserveWidgets):
    def remove(self, k): raise RuntimeError("remove failed: " + k)


class _RaiseTextWidgets(_PreserveWidgets):
    def text(self, k, v): raise RuntimeError("text failed: " + k)


def test_isolation_fail_closed_on_widget_remove_failure():
    """Reviewer probe #1: prior child A's key present; param-less child B;
    aidputils.widgets.remove() raises -> reset FAIL-CLOSED (no _ISO_OK), not a silent
    leak of A's value."""
    a = type("A", (), {})()
    a.widgets = _RaiseRemoveWidgets({"job": "J", "experiment_name": "M14"})  # M14 = A leak
    out = _run_guarded(jm._aidputils_widget_seed_setp({}),
                       {"aidputils": a, "_wid_orig": {"job": "J"}})
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_isolation_fail_closed_on_widget_text_failure():
    """Failed SEED: child B's own value cannot be written -> fail-closed."""
    a = type("A", (), {})()
    a.widgets = _RaiseTextWidgets({"job": "J"})
    out = _run_guarded(jm._aidputils_widget_seed_setp({"experiment_name": "M14"}),
                       {"aidputils": a, "_wid_orig": {"job": "J"}})
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_isolation_fail_closed_on_missing_widget_snapshot():
    """Snapshot unavailable while aidputils present -> reset RAISES -> fail-closed
    (was previously skipped = fail-open)."""
    a = _PreserveAidp({"experiment_name": "M14"})
    out = _run_guarded(jm._aidputils_widget_seed_setp({"experiment_name": "M15"}),
                       {"aidputils": a})  # NO _wid_orig
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


class _RaiseUnsetConf(_FakeConf):
    def unset(self, k): raise RuntimeError("unset failed: " + k)


class _RaiseSetConf(_FakeConf):
    def set(self, k, v): raise RuntimeError("set failed: " + k)


def test_native_scala_isolation_fail_closed_on_widget_clear_failure():
    a = _PreserveAidp()
    a.widgets = _RaiseRemoveWidgets({'experiment_name': 'M14'})
    out = _run_guarded(jm._aidputils_widget_seed_setp({}),
                       {'aidputils': a, '_wid_orig': {}})
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_native_scala_isolation_fail_closed_on_widget_seed_failure():
    a = _PreserveAidp()
    a.widgets = _RaiseTextWidgets()
    out = _run_guarded(jm._aidputils_widget_seed_setp({'experiment_name': 'M14'}),
                       {'aidputils': a, '_wid_orig': {}})
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_isolation_fail_closed_on_manifest_write_failure(tmp_path):
    """Failed manifest_params snapshot/write (reviewer-listed): the write is
    correctness-critical and un-guarded, so a failure propagates -> fail-closed.
    Simulated by pointing the manifest path under a regular FILE (makedirs raises)."""
    afile = tmp_path / "afile"
    afile.write_text("x")
    mp_path = str(afile / "manifest_params.json")   # parent is a file -> makedirs raises
    out = _run_guarded(jm._manifest_params_perchild_setp(mp_path, {"experiment_name": "M14"}),
                       {"_mp_orig_content": None})
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_isolation_guarded_ok_marker_on_clean_setup():
    a = _PreserveAidp({'job': 'J'})
    out = _run_guarded(jm._aidputils_widget_seed_setp({'experiment_name': 'M14'}),
                       {'aidputils': a, '_wid_orig': {'job': 'J'}})
    assert jm._ISO_OK in out and jm._ISO_FAIL not in out


def test_callers_fail_closed_check_iso_marker_and_skip_inline():
    """Source-level guard that BOTH inline callers act on the marker: they wrap the
    setp with `_isolation_guarded`, check for `_ISO_OK`, and on absence increment
    `cells_failed` + `continue` (do NOT reach `_inline_child_notebook`)."""
    src = (_SCRIPTS / "job_migrate.py").read_text(encoding="utf-8")
    assert src.count("_isolation_guarded(_setp)") >= 2, \
        "both inline callers must wrap the isolation setp with _isolation_guarded"
    # Callers use the collision-safe `_isolation_ok()` helper (standalone-line match +
    # `_ISO_FAIL` reject), NOT a bare `_ISO_OK not in` substring check (PR#150 review —
    # marker collision when a param key is literally named `__AIDP_ISO_OK__`).
    assert src.count("if not _isolation_ok(_iso_out):") >= 2, \
        "both callers must fail-closed via _isolation_ok() when isolation-OK is absent"
    assert "if _ISO_OK not in _iso_out:" not in src, \
        "the unsafe bare-substring marker check must be gone (marker-collision fix)"
    # The old fail-open (log a warning then fall through to inline) is removed from the
    # two 3-store (direct + cohort) callers. At most the separate legacy %scala
    # notebook.run path (Scala try/catch, not in scope for this finding) may retain it.
    assert src.count("notebook.run param-set warning") <= 1, \
        "the two 3-store inline callers must no longer swallow isolation failure as warning-and-continue"


# ── State VERIFICATION (PR#150 review round-4 redesign) ──────────────────────────
# The class the reviewer kept finding ("a swallowed op / native text()-preserve / bad
# baseline silently leaves a prior child's value while every op 'succeeded'") is closed
# by verifying the OUTCOME: after reset+seed, `_isolation_verify_setp` reads every store
# back and RAISES on any mismatch, so `_isolation_guarded` emits `_ISO_FAIL`. These pin
# clean→OK plus each contamination class → FAIL, independent of which op misbehaved.

def _full_setp(params, mp_path):
    return (jm._aidputils_widget_seed_setp(params)
            + jm._manifest_params_perchild_setp(mp_path, params)
            + jm._isolation_verify_setp(params, mp_path))


def _exec_full(params, ns, mp_path):
    buf = _io.StringIO()
    with _ctx.redirect_stdout(buf):
        exec(jm._isolation_guarded(_full_setp(params, mp_path)), dict(ns))
    return buf.getvalue()


def _clean_ns(tmp_path, wid=None, spark_init=None, mp_baseline=None):
    import json
    mp = tmp_path / "manifest_params.json"
    mp.write_text(json.dumps(mp_baseline or {"job": "J"}))
    return {
        "aidputils": _PreserveAidp(wid if wid is not None else {"job": "J"}),
        "spark": _FakeSpark(spark_init if spark_init is not None else {"spark.fixture.job": "J"}),
        "_wid_orig": dict(wid if wid is not None else {"job": "J"}),
        "_unused_spark_snapshot": {},
        "_mp_orig_content": json.dumps(mp_baseline or {"job": "J"}),
    }, str(mp)


def test_verify_clean_state_emits_ok(tmp_path):
    ns, mp = _clean_ns(tmp_path)
    out = _exec_full({"experiment_name": "M14"}, ns, mp)
    assert jm._ISO_OK in out and jm._ISO_FAIL not in out


def test_verify_catches_widget_stale_value_from_broken_remove(tmp_path):
    """#4: a swallowed remove + native text()-preserve leaves the seeded key holding a
    PRIOR child's value; every op 'succeeded' but the state is wrong -> verify FAILs."""
    import json
    class _BrokenRemove(_PreserveWidgets):
        def remove(self, k): pass  # swallow -> stale key survives -> text() preserves it
    a = _PreserveAidp(); a.widgets = _BrokenRemove({"job": "J", "experiment_name": "STALE"})
    mp = tmp_path / "manifest_params.json"; mp.write_text(json.dumps({"job": "J"}))
    ns = {"aidputils": a, "spark": _FakeSpark(), "_wid_orig": {"job": "J"},
          "_unused_spark_snapshot": {}, "_mp_orig_content": json.dumps({"job": "J"})}
    out = _exec_full({"experiment_name": "M14"}, ns, str(mp))
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_verify_catches_widget_leak_uncleared_prior_key(tmp_path):
    """A prior child's key that the reset failed to clear (broken remove) -> leak -> FAIL."""
    import json
    class _BrokenRemove(_PreserveWidgets):
        def remove(self, k): pass
    a = _PreserveAidp(); a.widgets = _BrokenRemove({"job": "J", "leaked": "PREV"})
    mp = tmp_path / "manifest_params.json"; mp.write_text(json.dumps({"job": "J"}))
    ns = {"aidputils": a, "spark": _FakeSpark(), "_wid_orig": {"job": "J"},
          "_unused_spark_snapshot": {}, "_mp_orig_content": json.dumps({"job": "J"})}
    out = _exec_full({"experiment_name": "M14"}, ns, str(mp))
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_verify_fails_on_missing_snapshot(tmp_path):
    """Snapshot failure (any store baseline absent) -> fail-closed, not fall-back-to-empty."""
    import json
    mp = tmp_path / "manifest_params.json"; mp.write_text(json.dumps({"job": "J"}))
    # widget baseline missing (aidputils present, _wid_orig None)
    ns = {"aidputils": _PreserveAidp({"job": "J"}), "spark": _FakeSpark(),
          "_wid_orig": None, "_unused_spark_snapshot": {}, "_mp_orig_content": json.dumps({"job": "J"})}
    out = _exec_full({"experiment_name": "M14"}, ns, str(mp))
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_native_scala_verify_catches_uncleared_widget_leak(tmp_path):
    class BrokenRemove(_PreserveWidgets):
        def remove(self, key): pass
    ns, mp = _clean_ns(tmp_path)
    ns['aidputils'].widgets = BrokenRemove({'job': 'J', 'experiment_name': 'STALE'})
    out = _exec_full({}, ns, mp)
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_native_scala_isolation_fail_closed_on_widget_read_error(tmp_path):
    class GetErrorWidgets(_PreserveWidgets):
        def getAll(self): raise RuntimeError('backend widget read error')
    a = _PreserveAidp()
    a.widgets = GetErrorWidgets()
    out = _run_guarded(jm._aidputils_widget_seed_setp({'experiment_name': 'M14'}),
                       {'aidputils': a, '_wid_orig': {}})
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_native_scala_absent_widget_baseline_key_is_not_a_false_failure(tmp_path):
    a = _PreserveAidp()
    out = _run_guarded(jm._aidputils_widget_seed_setp({'experiment_name': 'M14'}),
                       {'aidputils': a, '_wid_orig': {}})
    assert jm._ISO_OK in out and jm._ISO_FAIL not in out
    assert a.widgets.get('experiment_name') == 'M14'


def test_verify_empty_params_catches_unrestored_leak(tmp_path):
    """#4: the post-loop restore now appends verify({}). A baseline key left holding a
    child's value (a broken restore under native text()-preserve) must FAIL."""
    import json
    a = _PreserveAidp({"job": "J", "experiment_name": "CHILDVAL"})  # not in baseline -> leak
    mp = tmp_path / "manifest_params.json"; mp.write_text(json.dumps({"job": "J"}))
    ns = {"aidputils": a, "spark": _FakeSpark(), "_wid_orig": {"job": "J"},
          "_unused_spark_snapshot": {}, "_mp_orig_content": json.dumps({"job": "J"})}
    buf = _io.StringIO()
    with _ctx.redirect_stdout(buf):
        exec(jm._isolation_guarded(jm._isolation_verify_setp({}, str(mp))), ns)
    out = buf.getvalue()
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out


def test_orchestration_child_not_inlined_when_isolation_fails(monkeypatch, tmp_path):
    """Caller BEHAVIOUR (PR#150 review r4, o-vazunnavar): if per-child isolation setup
    does not report success (`_ISO_OK` absent — a reset/seed failed), the child MUST
    NOT be inlined — running it would read a prior child's value. Drive the REAL
    `process_notebook` notebook.run path with a kernel that withholds the isolation
    marker, and assert the child body never executes and the cell is recorded failed."""
    from test_child_exit_conversion import _run, _ExitKernel

    _orig = _ExitKernel.execute

    async def _no_iso_marker(self, code, **kwargs):
        if jm._ISO_OK in code and "_task_expected =" not in code:  # fail child setup only
            self.executed.append(code)          # setp is still SENT ...
            return {"status": "ok", "outputs": []}   # ... but returns NO OK marker (failure)
        return await _orig(self, code, **kwargs)

    monkeypatch.setattr(_ExitKernel, "execute", _no_iso_marker)

    notebooks = {"childA": ['CHILD_BODY_MARKER = dbutils.widgets.get("experiment_name")']}
    parent = ['dbutils.notebook.run("/Workspace/childA", 3600, {"experiment_name": "M14"})']
    result, saved, kernel, installs, logs = _run(monkeypatch, tmp_path, notebooks, parent=parent)

    executed = "\n".join(kernel.executed)
    assert "CHILD_BODY_MARKER" not in executed, \
        "child was inlined despite isolation failure — must fail-closed (not run the child)"
    assert any("isolation" in c.get("note", "").lower() and c.get("status") == "fail"
               for c in result["cell_results"]), \
        "isolation failure must be recorded as a cell failure"

    # #1 (review r4): the saved parent notebook must NOT contain the notebook.run cell
    # twice. The migrated cell is appended once for every cell; the fail-closed skip must
    # not append the original cell again (that produced a duplicate/stale cell).
    import json as _json
    _parent_saved = None
    for _p, _content in kernel.files.items():
        if str(_p).endswith("parent.ipynb"):
            try:
                _parent_saved = _json.loads(_content if isinstance(_content, str)
                                            else _content.decode("utf-8"))
            except Exception:
                pass
    assert _parent_saved is not None, "parent notebook was not saved"
    _nbrun_cells = [c for c in _parent_saved.get("cells", [])
                    if "notebook.run" in "".join(c.get("source", []))]
    assert len(_nbrun_cells) == 1, \
        f"duplicate/stale notebook.run cell saved (review r4 #1): found {len(_nbrun_cells)}"


# ── #1 marker collision + #2 P2 anchored-FAIL (PR#150 review) ─────────────────────
# The caller used to read success with `_ISO_OK in output`. But `_ISO_OK` ("__AIDP_ISO_OK__")
# is a SUBSTRING of any `_ISO_FAIL` line that names a param key literally called
# `__AIDP_ISO_OK__` (e.g. "__AIDP_ISO_FAIL__ ... mismatch for __AIDP_ISO_OK__"), so a
# genuine failure read as success (fail-OPEN). Final contract: `_isolation_ok()` returns
# True iff the output's LAST line ENDS WITH the OK marker (tolerates the native-widget echo
# gluing it to the previous line, no trailing newline) AND no line is an ANCHORED `_ISO_FAIL`
# marker (a line that, stripped, STARTS with the sentinel — NOT an arbitrary substring, so an
# echoed widget VALUE equal to `__AIDP_ISO_FAIL__` does not false-fail; review P2).

def test_isolation_ok_helper_unit():
    """`_isolation_ok`: output whose LAST line ENDS WITH the OK marker -> True; empty/None ->
    False; an ANCHORED `_ISO_FAIL` marker line -> False, even when it embeds the OK token."""
    assert jm._isolation_ok(jm._ISO_OK) is True
    assert jm._isolation_ok("noise\n" + jm._ISO_OK) is True
    assert jm._isolation_ok("noise\n" + jm._ISO_OK + "\n") is True   # trailing newline tolerated
    assert jm._isolation_ok("") is False
    assert jm._isolation_ok(None) is False
    # the marker is always the guarded block's LAST print; content after it never happens on
    # the OK path, so a marker followed by more text is not success.
    assert jm._isolation_ok("prefix " + jm._ISO_OK + " suffix") is False
    # a FAIL record that EMBEDS the OK token (the collision) must read as failure
    collide = jm._ISO_FAIL + " iso-verify: widget missing/mismatch for " + jm._ISO_OK
    assert jm._isolation_ok(collide) is False


def test_isolation_ok_tolerates_glued_marker_from_native_widget_echo():
    """LIVE regression (2026-09-25, cluster c91dc735): native aidputils widget ops echo a
    JSON blob to stdout with NO trailing newline, so the guarded block's `print` glues the
    marker onto the blob's line (`…}}]__AIDP_ISO_OK__`). A standalone-line match false-failed
    and skipped a genuinely-isolated child; endswith must accept it."""
    glued = ('[{"value": "M187", "label": "experiment_name", "version": 1, '
             '"updatedAt": "2026-09-25T13:34:33.028Z", "updatedBy": "ocid1.user.oc1..xyz"}}]'
             + jm._ISO_OK)
    assert jm._isolation_ok(glued) is True
    # …but a glued marker inside a FAIL record is still a failure (collision-safe).
    glued_fail = "blob}}]" + jm._ISO_FAIL + " RuntimeError('x for " + jm._ISO_OK + "')"
    assert jm._isolation_ok(glued_fail) is False


def test_isolation_guarded_emits_marker_on_its_own_line_after_no_newline_stdout():
    """The emitter prefixes the marker with a newline so it starts a fresh line even when the
    setp's last stdout has no trailing newline; `_isolation_ok` then accepts it."""
    import sys as _sys
    # setp writes to stdout WITHOUT a trailing newline, mimicking the native widget echo
    setp = "import sys as _s\n_s.stdout.write('widget-echo-no-newline')\n"
    out = _run_guarded(setp, {})
    assert jm._isolation_ok(out) is True
    assert jm._ISO_FAIL not in out


def test_isolation_ok_not_fooled_by_fail_sentinel_as_echoed_value():
    """PR#150 review P2: a widget/param VALUE equal to `__AIDP_ISO_FAIL__`, echoed inside the
    native widget JSON blob before a valid OK marker, must NOT false-fail — `_ISO_FAIL` is
    matched only as an ANCHORED marker line, never as an arbitrary substring."""
    # sentinel carried as a VALUE mid-line (JSON echo), then the guard's OK marker on its line
    out = '[{"value": "' + jm._ISO_FAIL + '", "label": "experiment_name"}]\n' + jm._ISO_OK
    assert jm._isolation_ok(out) is True
    # same, glued (no newline before the OK marker)
    out2 = '[{"value": "' + jm._ISO_FAIL + '", "label": "x"}]' + jm._ISO_OK
    assert jm._isolation_ok(out2) is True
    # …but a GENUINE anchored FAIL marker line is still a failure even if an OK line follows
    out3 = jm._ISO_FAIL + " RuntimeError('boom')\n" + jm._ISO_OK
    assert jm._isolation_ok(out3) is False


def test_isolation_ok_end_to_end_param_named_like_ok_marker(tmp_path):
    """A real guarded verify FAILURE whose failing key is literally named `__AIDP_ISO_OK__`
    emits a `_ISO_FAIL` line embedding the OK token. A bare-substring check would call it
    success; `_isolation_ok()` must call it failure (fail-closed)."""
    import json
    key = jm._ISO_OK  # param key collides with the success marker
    # current widget store is MISSING the seeded key -> verify raises "missing/mismatch for <key>"
    a = _PreserveAidp({"job": "J"})
    mp = tmp_path / "manifest_params.json"; mp.write_text(json.dumps({"job": "J"}))
    ns = {"aidputils": a, "spark": _FakeSpark(), "_wid_orig": {"job": "J"},
          "_unused_spark_snapshot": {}, "_mp_orig_content": json.dumps({"job": "J"})}
    buf = _io.StringIO()
    with _ctx.redirect_stdout(buf):
        exec(jm._isolation_guarded(jm._isolation_verify_setp({key: "M14"}, str(mp))), ns)
    out = buf.getvalue()
    assert jm._ISO_FAIL in out and key in out            # the collision is present in output
    assert jm._isolation_ok(out) is False                # …and still read as failure


# ── #2 missing/dropped baseline key (PR#150 review) ──────────────────────────────
# The old widget+manifest verify checked only (a) each seeded key reads back and (b) no
# EXTRA key remains. It missed a baseline key DROPPED by a broken reset (present in the
# baseline snapshot, absent from the resulting store) — a real between-child contamination.
# Verify is now BIDIRECTIONAL: expected == baseline overlaid with the child's params, and
# Every expected widget and manifest key must read back, including baseline keys.

def test_verify_catches_dropped_widget_baseline_key(tmp_path):
    """#2: a baseline widget key present in the snapshot but absent from the current store
    (dropped by a broken reset) must FAIL; the seeded key + no-extras still hold."""
    import json
    # current store has the seeded key + one baseline key, but the 'env' baseline key is GONE
    a = _PreserveAidp({"job": "J", "experiment_name": "M14"})
    mp = tmp_path / "manifest_params.json"; mp.write_text(json.dumps({"job": "J", "env": "prod"}))
    ns = {"aidputils": a, "spark": _FakeSpark(),
          "_wid_orig": {"job": "J", "env": "prod"},          # baseline HAS env
          "_unused_spark_snapshot": {}, "_mp_orig_content": json.dumps({"job": "J", "env": "prod"})}
    buf = _io.StringIO()
    with _ctx.redirect_stdout(buf):
        exec(jm._isolation_guarded(jm._isolation_verify_setp({"experiment_name": "M14"}, str(mp))), ns)
    out = buf.getvalue()
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out
    assert "env" in out                                       # names the dropped key


def test_verify_catches_dropped_manifest_baseline_key(tmp_path):
    """#2: a baseline manifest key present in the snapshot but absent from the current file
    (dropped by a broken reset) must FAIL."""
    import json
    mp = tmp_path / "manifest_params.json"
    # current file is MISSING baseline key 'env'; seeded key present, no extras
    mp.write_text(json.dumps({"job": "J", "experiment_name": "M14"}))
    ns = {"aidputils": _PreserveAidp({"job": "J", "env": "prod", "experiment_name": "M14"}),
          "spark": _FakeSpark({"spark.fixture.experiment_name": "M14"}),  # spark store OK
          "_wid_orig": {"job": "J", "env": "prod"},
          "_unused_spark_snapshot": {},
          "_mp_orig_content": json.dumps({"job": "J", "env": "prod"})}  # baseline HAS env
    buf = _io.StringIO()
    with _ctx.redirect_stdout(buf):
        exec(jm._isolation_guarded(jm._isolation_verify_setp({"experiment_name": "M14"}, str(mp))), ns)
    out = buf.getvalue()
    assert jm._ISO_FAIL in out and jm._ISO_OK not in out
    assert "env" in out


def test_verify_clean_state_with_baseline_key_passes(tmp_path):
    """Positive control for #2: when the baseline key IS present in both stores, the
    bidirectional check passes (no false failure)."""
    import json
    a = _PreserveAidp({"job": "J", "env": "prod", "experiment_name": "M14"})
    mp = tmp_path / "manifest_params.json"
    mp.write_text(json.dumps({"job": "J", "env": "prod", "experiment_name": "M14"}))
    ns = {"aidputils": a, "spark": _FakeSpark({"spark.fixture.experiment_name": "M14"}),
          "_wid_orig": {"job": "J", "env": "prod"},
          "_unused_spark_snapshot": {}, "_mp_orig_content": json.dumps({"job": "J", "env": "prod"})}
    buf = _io.StringIO()
    with _ctx.redirect_stdout(buf):
        exec(jm._isolation_guarded(jm._isolation_verify_setp({"experiment_name": "M14"}, str(mp))), ns)
    out = buf.getvalue()
    assert jm._ISO_OK in out and jm._ISO_FAIL not in out
