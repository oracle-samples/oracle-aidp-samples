"""Unit tests for the cluster-session bootstrap oidlUtils wrapper.

Mirrors the actual bootstrap snippet in scripts/job_migrate_from_workflow.py.
The wrapper survives cluster-session recycles because the snippet is registered
with cluster_session and replayed on every fresh kernel.

Design (validated against the real AIDP kernel — see /tmp/test_bootstrap_snippet_aidp.py):

  - `oidlUtils` is `OidlUtilsWrapper`, `.parameters` is a read-only property
    of that class wrapping a py4j `JavaObject`. We MUST replace the `oidlUtils`
    global (not its .parameters attribute) to intercept getParameter calls.

  - The wrapper's `_AIDPParams.getParameter` reads from two files:
      1. task_values.json    — written by setTaskValue (cross-task, mutable)
      2. manifest_params.json — written by process_notebook at task start
                                (per-task, read-only for the wrapper)
    Lookup order: task_values → manifest → caller default.

  - Idempotency: `type(oidlUtils).__name__ == '_AIDPOidlWrapper'` so replay on
    recycle does not double-wrap.

Tests:
  1. Initial install — wrapper replaces global, prints OK verdict
  2. Customer pattern eval(getParameter('models', '')) returns manifest value
  3. setTaskValue writes to task_values.json
  4. setTaskValue value takes precedence over manifest value (lookup order)
  5. Missing key falls through to caller default
  6. Idempotency — re-running the snippet detects ALREADY_WRAPPED
  7. NameError safety — snippet without oidlUtils in scope
  8. Read-only .parameters property — bridge works without touching it
  9. Passthrough — oidlUtils.notebook still works via __getattr__
 10. Manifest file mutation between calls is picked up (no caching)
"""
import contextlib
import io
import json
import os
import sys
import tempfile
import traceback


# ───────────── Fake oidlUtils that mirrors the real AIDP shape ────────────
class _FakeJavaMember:
    """Mimics py4j.java_gateway.JavaMember — returned for ANY attribute access.
    Always truthy."""
    def __bool__(self):
        return True
    def __repr__(self):
        return "<FakeJavaMember>"


class _FakeJavaObject:
    """Mimics py4j JavaObject — `getattr(obj, anything, default)` returns a
    JavaMember (truthy) rather than the default."""
    def __init__(self):
        pass
    def getTaskValue(self, name):
        return f"taskvalue:{name}"
    def getAdditionalParam(self, name):
        return f"additional:{name}"
    def __getattr__(self, attr):
        return _FakeJavaMember()


def _make_fake_oidl():
    """Build a fake OidlUtilsWrapper matching AIDP's real shape:
      - `.parameters` is a read-only property
      - The value is a py4j-like JavaObject"""
    fake_params = _FakeJavaObject()
    fake_notebook = _FakeJavaObject()

    class _FakeOidlWrapper:
        @property
        def parameters(self):
            return fake_params
        @property
        def notebook(self):
            return fake_notebook

    return _FakeOidlWrapper()


# ───────────────────────── Snippet template (mirrors production) ──────────
def build_snippet(tv_file, mp_file):
    """The bootstrap snippet exactly as it will appear in production."""
    return f"""
import json, os

_TV_FILE = {json.dumps(tv_file)}
_MP_FILE = {json.dumps(mp_file)}
os.makedirs(os.path.dirname(_TV_FILE), exist_ok=True)

class _AIDPParams:
    def __init__(self, native):
        self._native = native
    def _read(self, path):
        if os.path.exists(path):
            try:
                with open(path) as f:
                    return json.load(f)
            except Exception:
                return {{}}
        return {{}}
    def getParameter(self, name, default=""):
        tv = self._read(_TV_FILE)
        if name in tv:
            return tv[name]
        mp = self._read(_MP_FILE)
        if name in mp:
            return mp[name]
        return default
    def setTaskValue(self, name, value):
        data = self._read(_TV_FILE)
        data[name] = value
        with open(_TV_FILE, 'w') as f:
            json.dump(data, f)
    def __getattr__(self, attr):
        return getattr(self._native, attr)

class _AIDPOidlWrapper:
    def __init__(self, native):
        self._native = native
        self.parameters = _AIDPParams(native.parameters)
    def __getattr__(self, attr):
        return getattr(self._native, attr)

if type(oidlUtils).__name__ != '_AIDPOidlWrapper':
    oidlUtils = _AIDPOidlWrapper(oidlUtils)
    print('[AIDP-BRIDGE] OK (INSTALLED, tv=' + _TV_FILE + ', mp=' + _MP_FILE + ')')
else:
    print('[AIDP-BRIDGE] OK (ALREADY_WRAPPED)')
"""


def _install(oidl, tv_file, mp_file, capture=True):
    """Exec the snippet in a sandbox namespace. Returns (namespace, stdout)."""
    namespace = {"oidlUtils": oidl} if oidl is not None else {}
    buf = io.StringIO()
    snippet = build_snippet(tv_file, mp_file)
    if capture:
        with contextlib.redirect_stdout(buf):
            exec(snippet, namespace)
    else:
        exec(snippet, namespace)
    return namespace, buf.getvalue()


def _write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


def _make_paths(tmpdir, label):
    tv = os.path.join(tmpdir, f"{label}_task_values.json")
    mp = os.path.join(tmpdir, f"{label}_manifest_params.json")
    return tv, mp


# ───────────────────────────────── Tests ──────────────────────────────────
def test_1_initial_install(tmpdir):
    tv, mp = _make_paths(tmpdir, "t1")
    _write_json(mp, {"models": "['A']"})
    oidl = _make_fake_oidl()
    ns, out = _install(oidl, tv, mp)
    assert type(ns["oidlUtils"]).__name__ == "_AIDPOidlWrapper"
    assert "[AIDP-BRIDGE] OK (INSTALLED" in out
    print("PASS 1: wrapper installed, prints INSTALLED verdict")


def test_2_customer_pattern(tmpdir):
    tv, mp = _make_paths(tmpdir, "t2")
    _write_json(mp, {"models": "['METRICV5_RETRAIN', 'VALUEV4_RETRAIN']"})
    oidl = _make_fake_oidl()
    ns, _ = _install(oidl, tv, mp)
    # Customer's failing pattern: eval(getParameter('models', ""))
    models = eval(ns["oidlUtils"].parameters.getParameter("models", ""))
    assert models == ["METRICV5_RETRAIN", "VALUEV4_RETRAIN"]
    print("PASS 2: customer eval(getParameter('models', '')) returns manifest value")


def test_3_setTaskValue_writes_to_file(tmpdir):
    tv, mp = _make_paths(tmpdir, "t3")
    _write_json(mp, {"models": "['MANIFEST']"})
    oidl = _make_fake_oidl()
    ns, _ = _install(oidl, tv, mp)
    ns["oidlUtils"].parameters.setTaskValue("a_key", "a_value")
    # Disk check
    with open(tv) as f:
        data = json.load(f)
    assert data == {"a_key": "a_value"}
    print("PASS 3: setTaskValue persists to task_values.json")


def test_4_task_value_overrides_manifest(tmpdir):
    """task_values takes precedence over manifest_params for the same key."""
    tv, mp = _make_paths(tmpdir, "t4")
    _write_json(mp, {"models": "['MANIFEST_VALUE']"})
    oidl = _make_fake_oidl()
    ns, _ = _install(oidl, tv, mp)
    # Override via setTaskValue
    ns["oidlUtils"].parameters.setTaskValue("models", "['OVERRIDE_VALUE']")
    result = ns["oidlUtils"].parameters.getParameter("models", "")
    assert result == "['OVERRIDE_VALUE']", f"got {result!r}"
    print("PASS 4: setTaskValue takes precedence over manifest (correct lookup order)")


def test_5_missing_key_default(tmpdir):
    tv, mp = _make_paths(tmpdir, "t5")
    _write_json(mp, {"models": "['A']"})
    oidl = _make_fake_oidl()
    ns, _ = _install(oidl, tv, mp)
    result = ns["oidlUtils"].parameters.getParameter("not_in_any_file", "MY_DEFAULT")
    assert result == "MY_DEFAULT", f"got {result!r}"
    print("PASS 5: missing key falls through to caller default")


def test_6_idempotent_double_install(tmpdir):
    tv, mp = _make_paths(tmpdir, "t6")
    _write_json(mp, {"models": "['A']"})
    oidl = _make_fake_oidl()
    ns, out1 = _install(oidl, tv, mp)
    assert "[AIDP-BRIDGE] OK (INSTALLED" in out1
    # Second install on the wrapped object — must detect ALREADY_WRAPPED
    wrapped = ns["oidlUtils"]
    ns2, out2 = _install(wrapped, tv, mp)
    assert "[AIDP-BRIDGE] OK (ALREADY_WRAPPED)" in out2, f"unexpected: {out2!r}"
    # Probe still works
    assert ns2["oidlUtils"].parameters.getParameter("models", "") == "['A']"
    print("PASS 6: double install detected via class-name check, probe still works")


def test_7_no_oidlutils_in_scope(tmpdir):
    """If oidlUtils isn't in scope, the snippet raises NameError. The cluster
    session's snippet replay catches all exceptions per-snippet so the replay
    continues — we just need to verify the snippet itself does raise."""
    tv, mp = _make_paths(tmpdir, "t7")
    snippet = build_snippet(tv, mp)
    namespace = {}
    raised = False
    try:
        exec(snippet, namespace)
    except NameError:
        raised = True
    assert raised, "expected NameError when oidlUtils missing"
    print("PASS 7: NameError raised when oidlUtils missing (cluster session catches it)")


def test_8_readonly_parameters_property(tmpdir):
    """Real AIDP oidlUtils.parameters has no setter — direct assignment raises
    AttributeError. The bridge MUST NOT touch oidlUtils.parameters; it replaces
    oidlUtils itself. Confirm bridge installs cleanly with a strict read-only fake."""
    tv, mp = _make_paths(tmpdir, "t8")
    _write_json(mp, {"models": "['A']"})

    class _StrictReadOnlyOidl:
        @property
        def parameters(self):
            return _FakeJavaObject()
        @parameters.setter
        def parameters(self, _):
            raise AttributeError("read-only property")
        @property
        def notebook(self):
            return _FakeJavaObject()

    strict = _StrictReadOnlyOidl()
    ns, out = _install(strict, tv, mp)
    assert "[AIDP-BRIDGE] OK (INSTALLED" in out
    assert ns["oidlUtils"].parameters.getParameter("models", "") == "['A']"
    print("PASS 8: bridge works even when native .parameters has no setter")


def test_9_passthrough_attributes(tmpdir):
    """oidlUtils.notebook etc. must still work via __getattr__ proxy."""
    tv, mp = _make_paths(tmpdir, "t9")
    _write_json(mp, {"models": "['A']"})
    oidl = _make_fake_oidl()
    ns, _ = _install(oidl, tv, mp)
    nb = ns["oidlUtils"].notebook
    assert nb is not None
    # getTaskValue (read) and getAdditionalParam fall through native via _AIDPParams.__getattr__
    tv_val = ns["oidlUtils"].parameters.getTaskValue("foo")
    ap_val = ns["oidlUtils"].parameters.getAdditionalParam("models")
    assert tv_val == "taskvalue:foo", f"got {tv_val!r}"
    assert ap_val == "additional:models", f"got {ap_val!r}"
    print("PASS 9: .notebook, getTaskValue, getAdditionalParam pass through")


def test_10_manifest_mutation_picked_up(tmpdir):
    """No caching — if manifest_params.json changes on disk, next getParameter
    call sees the new value. Important for inter-task transitions."""
    tv, mp = _make_paths(tmpdir, "t10")
    _write_json(mp, {"models": "['INITIAL']"})
    oidl = _make_fake_oidl()
    ns, _ = _install(oidl, tv, mp)
    assert ns["oidlUtils"].parameters.getParameter("models", "") == "['INITIAL']"
    # Mutate the file (simulates process_notebook updating for a new task)
    _write_json(mp, {"models": "['UPDATED']"})
    assert ns["oidlUtils"].parameters.getParameter("models", "") == "['UPDATED']"
    print("PASS 10: manifest_params.json mutations are picked up on next call")


if __name__ == "__main__":
    tests = [
        test_1_initial_install,
        test_2_customer_pattern,
        test_3_setTaskValue_writes_to_file,
        test_4_task_value_overrides_manifest,
        test_5_missing_key_default,
        test_6_idempotent_double_install,
        test_7_no_oidlutils_in_scope,
        test_8_readonly_parameters_property,
        test_9_passthrough_attributes,
        test_10_manifest_mutation_picked_up,
    ]
    passed = 0
    with tempfile.TemporaryDirectory(prefix="aidp_bridge_tests_") as tmpdir:
        for t in tests:
            try:
                t(tmpdir)
                passed += 1
            except Exception as e:
                print(f"FAIL {t.__name__}: {e}")
                traceback.print_exc()
    print(f"\n=== {passed}/{len(tests)} tests passed ===")
    sys.exit(0 if passed == len(tests) else 1)
