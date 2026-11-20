"""Deterministic local-/tmp write redirect (job_migrate._ensure_tmp_local_redirect).

Incident 2026-09-16 (AutoRefit/examplevendor): a raw `plt.savefig(f'/tmp/simulation_budget_{campaign}.png')`
inside a helper called in a loop intermittently raised `PermissionError [Errno 13]`
— a prior run under a different per-session UID had left that exact file behind and
it could not be overwritten. The /tmp→per-session-dir redirect used to be LLM prompt
guidance (non-deterministic, occasionally missed). This suite pins the deterministic
replacement: broad coverage of /tmp WRITE paths (across sinks and path forms) plus a
runtime-confined helper (nested dirs + escape refusal), while read-only cross-notebook
paths are left raw.
"""

import os

import pytest

from scripts.job_migrate import (
    _collect_tmp_write_keys,
    _rewrite_tmp_local_writes,
    _ensure_tmp_local_redirect,
    _redirect_tmp_in_child_cells,
    _has_aidp_tmp_helper,
    _TMP_SETUP_SOURCE,
    _TMP_HELPER_NAME,
)

H = _TMP_HELPER_NAME  # "__aidp_tmp_redirect__"


def _redirect(src: str) -> str:
    return _rewrite_tmp_local_writes(src, _collect_tmp_write_keys(src))


def _code_cell(src: str) -> dict:
    return {"cell_type": "code", "metadata": {}, "outputs": [], "source": [src]}


def _joined(cells: list) -> str:
    return "\n".join("".join(c.get("source", [])) for c in cells)


# ── The incident case ────────────────────────────────────────────────────────

def test_fstring_savefig_inside_function_is_redirected():
    src = (
        "def sim_budget_tracker(campaign, flag):\n"
        "    plt.figure()\n"
        "    plt.savefig(f'/tmp/simulation_budget_{campaign}.png', dpi=100)\n"
    )
    out = _redirect(src)
    assert f"{H}(f'simulation_budget_{{campaign}}.png')" in out
    assert "'/tmp/simulation_budget" not in out
    assert "dpi=100" in out
    compile(out, "<t>", "exec")


def test_plain_literal_savefig_is_redirected():
    assert _redirect("plt.savefig('/tmp/fig.png')") == f"plt.savefig({H}('fig.png'))"


# ── Path-form coverage ───────────────────────────────────────────────────────

def test_variable_indirection_is_left_raw():
    # Variable indirection (p = '/tmp/x'; savefig(p)) is NOT analysed — a name-based
    # flow can't be resolved safely without full lexical scoping (a parameter or
    # reuse of the same name shadows it), so it is left raw and handled by the
    # prompt. This is the resolution for the two scope-crossing review findings.
    cells = [_code_cell("p = '/tmp/x.png'\nplt.savefig(p)\n")]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells])
    body = _joined(out)
    assert H not in body and "'/tmp/x.png'" in body


def test_writer_param_does_not_redirect_unrelated_input():
    # A read-only input in one function must NOT be redirected because a *writer*
    # in another function has a parameter of the same name (the second scope bug).
    src = (
        "def load_input():\n    p = '/tmp/external.csv'\n    return reader(p)\n\n"
        "def write_output(p):\n    df.to_csv(p)\n\nresult = load_input()\n"
    )
    out = _ensure_tmp_local_redirect([_code_cell(src)])
    assert H not in _joined(out) and "'/tmp/external.csv'" in _joined(out)


def test_read_only_variable_stays_raw():
    # p holds a /tmp path but only ever flows to a READ → not marked, left raw.
    cells = [_code_cell("p = '/tmp/r.csv'\ndf = pd.read_csv(p)\n")]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells])
    assert H not in _joined(out) and "'/tmp/r.csv'" in _joined(out)


def test_variable_reused_across_scopes_left_raw():
    # Same name `p` assigned in two functions (one read-only input, one written)
    # → >1 assignment site → NEITHER redirected. The read-only external input must
    # NOT be redirected (that was the P1b scope-crossing regression).
    src = (
        "def load_input():\n    p = '/tmp/external.csv'\n    return reader(p)\n\n"
        "def write_output():\n    p = '/tmp/output.csv'\n    df.to_csv(p)\n"
    )
    out = _ensure_tmp_local_redirect([_code_cell(src)])
    body = _joined(out)
    assert "'/tmp/external.csv'" in body and H not in body


def test_reassigned_variable_left_raw():
    # p reassigned (2 sites) → ambiguous → left raw.
    cells = [_code_cell("p = '/tmp/a.png'\np = compute()\nplt.savefig(p)\n")]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells])
    assert H not in _joined(out)


def test_docstring_matching_written_path_is_preserved():
    # A docstring equal to a written /tmp path must NOT become a helper call
    # (would drop __doc__). Only the real open() write is redirected.
    src = 'def f():\n    """/tmp/x"""\n    return 1\nopen("/tmp/x", "w").close()\n'
    out = _redirect(src)
    assert '"""/tmp/x"""' in out                    # docstring intact
    assert f'open({H}("x"), "w")' in out            # the write is redirected


def test_path_wrapped_literal_left_raw():
    # Path()/wrapper literals are NOT recursed (a wrapped dir-prefix can't be
    # redirected correctly) — left raw for the prompt.
    src = "plt.savefig(Path('/tmp/x.png'))"
    assert _redirect(src) == src


def test_os_path_join_left_raw():
    # os.path.join dir-prefix component is left raw (redirecting it would create
    # the wrong directory — the P2a runtime break).
    src = "import os\nplt.savefig(os.path.join('/tmp/d', f))\n"
    assert _redirect(src) == src


def test_nested_subdir_is_covered():
    # helper makedirs the parent, so nested paths are redirected (not skipped)
    assert _redirect("df.to_csv('/tmp/out/data.csv')") == f"df.to_csv({H}('out/data.csv'))"


# ── Keyword sink args + broad sink set ───────────────────────────────────────

@pytest.mark.parametrize("src,key", [
    ("plt.savefig(fname='/tmp/x.png')", "/tmp/x.png"),
    ("df.to_csv(path_or_buf='/tmp/x.csv')", "/tmp/x.csv"),
    ("np.save(file='/tmp/a.npy', arr=z)", "/tmp/a.npy"),
    ("open(file='/tmp/x', mode='w')", "/tmp/x"),
    ("torch.save(m, f='/tmp/m.pt')", "/tmp/m.pt"),
    ("joblib.dump(o, filename='/tmp/o.pkl')", "/tmp/o.pkl"),
])
def test_keyword_sink_args_collected(src, key):
    assert key in _collect_tmp_write_keys(src)


@pytest.mark.parametrize("src,key", [
    ("torch.save(model, '/tmp/m.pt')", "/tmp/m.pt"),        # arg1
    ("joblib.dump(obj, '/tmp/o.pkl')", "/tmp/o.pkl"),       # arg1
    ("shutil.copy(src, '/tmp/dest.bin')", "/tmp/dest.bin"), # dest arg1
    ("shutil.move(src, '/tmp/dest.bin')", "/tmp/dest.bin"),
    ("np.save('/tmp/a.npy', arr)", "/tmp/a.npy"),
    ("np.savetxt('/tmp/t.txt', arr)", "/tmp/t.txt"),
    ("cv2.imwrite('/tmp/img.png', im)", "/tmp/img.png"),
    ("img.save('/tmp/pil.png')", "/tmp/pil.png"),           # PIL/keras .save arg0
    ("model.save('/tmp/model.keras')", "/tmp/model.keras"),
    ("fig.write_image('/tmp/plot.png')", "/tmp/plot.png"),  # plotly
    ("df.to_parquet('/tmp/p.parquet')", "/tmp/p.parquet"),
    ("df.to_hdf('/tmp/s.h5', key='k')", "/tmp/s.h5"),
    ("open('/tmp/z.txt', 'a')", "/tmp/z.txt"),
])
def test_broad_write_sinks_collected(src, key):
    assert key in _collect_tmp_write_keys(src)


@pytest.mark.parametrize("src", [
    "pickle.dump(o, f)",         # file object, not a path
    "np.load('/tmp/a.npy')",     # read
    "torch.load('/tmp/m.pt')",   # read
    "pd.read_csv('/tmp/x.csv')", # read
    "open('/tmp/x', 'r')",       # read mode
    "df.to_json()",              # no path arg (returns a string)
    "tempfile.mkstemp(dir='/tmp')",    # unique name → no collision → not redirected
    "tempfile.mkdtemp(dir='/tmp/x')",  # directory sink → not redirected
])
def test_reads_and_non_path_calls_not_collected_as_writes(src):
    assert _collect_tmp_write_keys(src) == set()


# ── Reads: paired vs read-only ───────────────────────────────────────────────

def test_read_after_write_same_path_rewrites_both():
    out = _redirect("df.to_csv('/tmp/mid.csv')\ndf2 = pd.read_csv('/tmp/mid.csv')\n")
    assert out.count(f"{H}('mid.csv')") == 2
    assert "'/tmp/mid.csv'" not in out


def test_read_only_path_stays_raw():
    out = _rewrite_tmp_local_writes("shared = pd.read_csv('/tmp/from_other.csv')\n", set())
    assert out == "shared = pd.read_csv('/tmp/from_other.csv')\n"


def test_read_only_at_notebook_level_no_redirect():
    cells = [_code_cell("pd.read_csv('/tmp/from_other.csv')\n")]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells])
    assert len(out) == 1
    assert H not in _joined(out)


# ── Consistency: a self-written /tmp name is redirected at EVERY occurrence, in
#    ANY textual order, so it never splits across raw /tmp and the session dir.
#    (Regression guards for the reverted order-aware heuristic — PR #114.) ──

def test_read_before_write_across_cells_both_redirected():
    # A read textually BEFORE its producer write must still redirect — else the
    # file splits (read→raw /tmp, write→session dir → FileNotFound / stale read).
    cells = [
        _code_cell("prev = pd.read_csv('/tmp/data.csv')\n"),  # read first
        _code_cell("df.to_csv('/tmp/data.csv')\n"),           # producer write, later
    ]
    joined = _joined(_ensure_tmp_local_redirect([dict(c) for c in cells]))
    assert joined.count(f"{H}('data.csv')") == 2
    assert "'/tmp/data.csv'" not in joined


def test_write_then_read_across_cells_both_redirected():
    cells = [
        _code_cell("df.to_csv('/tmp/data.csv')\n"),           # producer write
        _code_cell("prev = pd.read_csv('/tmp/data.csv')\n"),  # read, after
    ]
    joined = _joined(_ensure_tmp_local_redirect([dict(c) for c in cells]))
    assert joined.count(f"{H}('data.csv')") == 2
    assert "'/tmp/data.csv'" not in joined


def test_helper_function_read_before_write_redirected():
    # `def load(): read('/tmp/x')` (cell 0) then write (cell 1): textual position
    # is not execution order, so both must redirect.
    cells = [
        _code_cell("def load():\n    return pd.read_csv('/tmp/x.csv')\n"),
        _code_cell("df.to_csv('/tmp/x.csv')\n"),
    ]
    joined = _joined(_ensure_tmp_local_redirect([dict(c) for c in cells]))
    assert joined.count(f"{H}('x.csv')") == 2
    assert "'/tmp/x.csv'" not in joined


def test_clean_before_write_os_remove_redirected():
    # os.remove of a self-written /tmp path must redirect too — otherwise a
    # prior-UID file makes os.remove raise PermissionError (the incident class
    # this redirect exists to prevent).
    src = ("import os\n"
           "try:\n    os.remove('/tmp/out.csv')\nexcept FileNotFoundError:\n    pass\n"
           "df.to_csv('/tmp/out.csv')\n")
    joined = _joined(_ensure_tmp_local_redirect([_code_cell(src)]))
    assert joined.count(f"{H}('out.csv')") == 2
    assert "'/tmp/out.csv'" not in joined


# ── Double-slash source ('/tmp//x' → relative 'x', not absolute '/x') ─────────

def test_double_slash_after_tmp_redirects_to_relative():
    assert _redirect("plt.savefig('/tmp//fig.png')") == f"plt.savefig({H}('fig.png'))"


def test_double_slash_fstring_redirects_to_relative():
    out = _redirect("plt.savefig(f'/tmp//img_{i}.png')")
    assert f"{H}(f'img_{{i}}.png')" in out
    assert "'/tmp//" not in out


# ── Key normalisation: slash spellings of the same file pair (write+read) ──────

def test_double_slash_write_pairs_single_slash_read():
    # '/tmp//a.csv' write and '/tmp/a.csv' read are the SAME file → both redirect.
    src = "df.to_csv('/tmp//a.csv')\nprev = pd.read_csv('/tmp/a.csv')\n"
    joined = _joined(_ensure_tmp_local_redirect([_code_cell(src)]))
    assert joined.count(f"{H}('a.csv')") == 2
    assert "'/tmp/a.csv'" not in joined and "'/tmp//a.csv'" not in joined


def test_dot_segment_write_pairs_plain_read():
    # '/tmp/./b.csv' write and '/tmp/b.csv' read key to the same file → both
    # redirect (runtime normpath collapses the './').
    src = "df.to_csv('/tmp/./b.csv')\nprev = pd.read_csv('/tmp/b.csv')\n"
    joined = _joined(_ensure_tmp_local_redirect([_code_cell(src)]))
    assert joined.count(f"{H}('") == 2  # both calls; not the helper `def` (no quote)
    assert "'/tmp/b.csv'" not in joined


# ── Left-untouched (anchoring, byte, concat, traversal) ──────────────────────

@pytest.mark.parametrize("src", [
    "df.to_csv('dbfs:/tmp/x.csv')",
    "df.to_csv('s3://bucket/tmp/x.csv')",
    "open('/tmpfoo/x', 'w')",
    "open('/var/tmp/x', 'w')",
    "plt.savefig('./tmp/fig.png')",
    "b = b'/tmp/x'\nopen(b, 'wb')",
    "plt.savefig('/tmp/' 'foo.png')",         # implicit concat → skipped
    "plt.savefig('/tmp/../../outside.png')",  # traversal → skipped statically
])
def test_left_untouched(src):
    assert _redirect(src) == src


# ── Non-ASCII (byte spans) ───────────────────────────────────────────────────

def test_non_ascii_path_rewrites_and_compiles():
    out = _redirect("plt.savefig('/tmp/café.png')\n")
    assert out == f"plt.savefig({H}('café.png'))\n"
    compile(out, "<t>", "exec")


def test_non_ascii_before_node_on_same_line():
    out = _redirect("läbel='x'; plt.savefig('/tmp/x.png')\n")
    assert f"{H}('x.png')" in out
    compile(out, "<t>", "exec")


# ── Robustness: magics, non-python, idempotency ──────────────────────────────

def test_line_magic_before_write():
    assert _redirect("%matplotlib inline\nplt.savefig('/tmp/fig.png')\n") == \
        f"%matplotlib inline\nplt.savefig({H}('fig.png'))\n"


def test_unparseable_cell_unchanged():
    src = "this is not python $$ /tmp/x.png"
    assert _rewrite_tmp_local_writes(src, {"/tmp/x.png"}) == src


def test_idempotent():
    once = _redirect("plt.savefig(f'/tmp/a_{i}.png')\n")
    twice = _rewrite_tmp_local_writes(once, _collect_tmp_write_keys(once))
    assert once == twice and H in once


# ── The injected runtime helper: confinement + nested makedirs ───────────────

def _load_helper(session_dir):
    """exec the injected setup with AIDP_NB_TMP pinned to `session_dir`."""
    prev = os.environ.get("AIDP_NB_TMP")
    os.environ["AIDP_NB_TMP"] = str(session_dir)
    try:
        ns = {}
        exec(compile(_TMP_SETUP_SOURCE, "<setup>", "exec"), ns)
        return ns[_TMP_HELPER_NAME], ns["_AIDP_TMP_DIR"]
    finally:
        if prev is None:
            os.environ.pop("AIDP_NB_TMP", None)
        else:
            os.environ["AIDP_NB_TMP"] = prev


def test_helper_flat_file(tmp_path):
    h, base = _load_helper(tmp_path / "sess")
    p = h("x.png")
    assert p == os.path.join(base, "x.png")
    assert os.path.isdir(base)


def test_helper_creates_nested_dirs(tmp_path):
    h, base = _load_helper(tmp_path / "sess")
    p = h("out/deep/data.csv")
    # normpath both sides — the helper normalizes separators (backslashes on Windows)
    assert os.path.normpath(p) == os.path.normpath(os.path.join(base, "out", "deep", "data.csv"))
    assert os.path.isdir(os.path.dirname(p))  # parent created


def test_helper_refuses_absolute_escape(tmp_path):
    h, _ = _load_helper(tmp_path / "sess")
    with pytest.raises(ValueError):
        h("/etc/passwd")


def test_helper_refuses_traversal_escape(tmp_path):
    h, _ = _load_helper(tmp_path / "sess")
    with pytest.raises(ValueError):
        h("../../outside")


# ── AST helper detection / collision safety ──────────────────────────────────

def test_docstring_mentioning_helper_does_not_suppress_setup():
    cells = [
        _code_cell(f'"""we call {H}( in docs"""\n'),
        _code_cell("plt.savefig('/tmp/x.png')\n"),
    ]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells])
    assert len(out) == 3 and _has_aidp_tmp_helper(out)


def test_customer_aidp_tmp_helper_does_not_collide():
    # A user helper named _aidp_tmp (the OLD name) must NOT be treated as ours.
    cells = [
        _code_cell("def _aidp_tmp(p):\n    return p\n"),
        _code_cell("plt.savefig('/tmp/x.png')\n"),
    ]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells])
    assert len(out) == 3  # our setup still injected
    assert _has_aidp_tmp_helper(out)


# ── Notebook-level insertion + idempotency ───────────────────────────────────

def test_setup_cell_injected_once():
    cells = [_code_cell("import x\n"), _code_cell("plt.savefig('/tmp/f.png')\n")]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells])
    assert len(out) == 3 and _has_aidp_tmp_helper(out)


def test_scala_native_notebook_is_noop():
    cells = [
        _code_cell("%scala\nval x = 1\n"),
        _code_cell('%scala\nprintln("/tmp/x")\n'),
    ]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells])
    assert len(out) == 2 and H not in _joined(out)


@pytest.mark.parametrize("lang", ["scala", "sql"])
def test_native_lang_skips_even_with_python_tmp_write(lang):
    # A Scala/SQL-default notebook with a %python /tmp write: the redirect is
    # SKIPPED so the Python setup cell can't be mis-promoted / run in the wrong
    # kernel. (Left to the prompt.)
    cells = [_code_cell("%python\nplt.savefig('/tmp/x.png')\n")]
    out = _ensure_tmp_local_redirect([dict(c) for c in cells], native_lang=lang)
    assert len(out) == 1 and H not in _joined(out)


def test_child_native_lang_skips():
    out, added = _redirect_tmp_in_child_cells(
        ["%python\nplt.savefig('/tmp/x.png')\n"], native_lang="scala")
    assert added == 0 and H not in "\n".join(out)


def test_notebook_level_idempotent_across_reruns():
    cells = [_code_cell("plt.savefig('/tmp/f.png')\n")]
    first = _ensure_tmp_local_redirect([dict(c) for c in cells])
    second = _ensure_tmp_local_redirect([dict(c) for c in first])
    assert len(first) == len(second)
    assert sum(1 for c in second if _has_aidp_tmp_helper([c])) == 1


# ── Child (%run / notebook.run) inline path ──────────────────────────────────

def test_child_setup_is_separate_cell():
    child = ["import numpy as np\n", "np.save('/tmp/child.npy', arr)\n"]
    out, added = _redirect_tmp_in_child_cells(list(child))
    assert added == 1 and len(out) == 3
    assert _has_aidp_tmp_helper([_code_cell(s) for s in out])
    assert f"{H}('child.npy')" in "\n".join(out)


def test_child_python_magic_preserved_on_line_1():
    out, added = _redirect_tmp_in_child_cells(["%python\nplt.savefig('/tmp/c.png')\n"])
    assert added == 1
    assert any(c.lstrip().startswith("%python") for c in out)
    assert f"{H}('c.png')" in "\n".join(out)


def test_child_without_tmp_write_untouched():
    child = ["import numpy as np\n", "x = np.load('/tmp/read.npy')\n"]
    out, added = _redirect_tmp_in_child_cells(list(child))
    assert added == 0 and out == child


def test_child_reredirect_is_idempotent():
    # simulate save-back re-run: running twice adds no second setup cell
    once, a1 = _redirect_tmp_in_child_cells(["plt.savefig('/tmp/c.png')\n"])
    twice, a2 = _redirect_tmp_in_child_cells(list(once))
    assert a2 == 0 and once == twice
