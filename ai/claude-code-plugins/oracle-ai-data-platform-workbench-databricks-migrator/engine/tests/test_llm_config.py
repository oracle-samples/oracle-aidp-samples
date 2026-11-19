"""Central LLM model/provider config — resolution, parsing, and call-site pins.

Contract: the model + provider come from `aidp_config` YAML (`llm.provider` /
`llm.model`) with NO silent code default — an absent/partial `llm` section fails
loudly. `thinking`/`max_tokens` are FIXED internal constants (not configurable —
PR#146). These tests pin that and guard against a call site regressing to a
hardcoded model string.
"""
from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import llm_config  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_llm_config():
    def _reset():
        llm_config.PROVIDER = None
        llm_config.MODEL = None
        llm_config.THINKING = llm_config.DEFAULT_THINKING
        llm_config.MAX_TOKENS = llm_config.DEFAULT_MAX_TOKENS
    _reset(); yield; _reset()


def _cfg(**llm):
    return types.SimpleNamespace(llm=types.SimpleNamespace(**llm))


def test_no_code_default_for_provider_or_model():
    # There is deliberately no baked-in model/provider; they must come from YAML.
    assert llm_config.MODEL is None and llm_config.PROVIDER is None
    # Only the behaviour params keep safe defaults.
    assert llm_config.DEFAULT_THINKING == {"type": "adaptive"}
    assert llm_config.DEFAULT_MAX_TOKENS == 16000


def test_configure_requires_llm_section():
    with pytest.raises(llm_config.LLMConfigError, match="llm"):
        llm_config.configure(types.SimpleNamespace())          # no .llm at all


def test_configure_requires_provider_and_model():
    with pytest.raises(llm_config.LLMConfigError, match="provider.*model|model"):
        llm_config.configure(_cfg(provider="anthropic"))       # model missing
    with pytest.raises(llm_config.LLMConfigError):
        llm_config.configure(_cfg(model="claude-opus-5-5"))    # provider missing


def test_configure_sets_from_yaml():
    llm_config.configure(_cfg(provider="anthropic", model="claude-opus-5-5"))
    assert llm_config.PROVIDER == "anthropic"
    assert llm_config.MODEL == "claude-opus-5-5"
    # thinking/max_tokens are fixed constants, not read from cfg.
    assert llm_config.THINKING == {"type": "adaptive"}
    assert llm_config.MAX_TOKENS == 16000


def test_configure_ignores_thinking_and_max_tokens_knobs():
    """PR#146: `thinking`/`max_tokens` are fixed internal constants — configure() must
    NOT read them from cfg even if a (stale) config carries them. The schema rejects
    them at load; this pins that configure() also does not apply them."""
    llm_config.configure(_cfg(provider="anthropic", model="claude-opus-5-5",
                              thinking="off", max_tokens=200000))
    assert llm_config.THINKING == {"type": "adaptive"}   # NOT disabled by 'off'
    assert llm_config.MAX_TOKENS == 16000                 # NOT the 200000 override


def test_configure_rejects_non_anthropic_provider():
    # Only anthropic is dispatched today — fail fast rather than accept a provider
    # no call site can honor (it would 500 later inside anthropic.Anthropic).
    with pytest.raises(llm_config.LLMConfigError, match="anthropic"):
        llm_config.configure(_cfg(provider="openai", model="gpt-4o"))


def test_ensure_configured_noop_when_already_set(monkeypatch):
    import aidp_config
    llm_config.MODEL = "claude-opus-5-5"          # pretend job_migrate already configured
    def _boom(**k):
        raise AssertionError("ensure_configured must not load when already configured")
    monkeypatch.setattr(aidp_config, "load_config", _boom)
    llm_config.ensure_configured()                # no-op
    assert llm_config.MODEL == "claude-opus-5-5"


def test_ensure_configured_loads_and_configures(monkeypatch):
    import aidp_config
    monkeypatch.setattr(aidp_config, "load_config",
                        lambda cli_config_path=None: _cfg(provider="anthropic",
                                                          model="claude-opus-5-5"))
    assert llm_config.MODEL is None               # unset until bootstrap
    llm_config.ensure_configured("/some/aidp_migrator.yaml")
    assert llm_config.MODEL == "claude-opus-5-5"
    assert llm_config.PROVIDER == "anthropic"


def test_ensure_configured_fail_loud_when_llm_absent(monkeypatch):
    import aidp_config, types as _t
    monkeypatch.setattr(aidp_config, "load_config",
                        lambda cli_config_path=None: _t.SimpleNamespace())  # no .llm
    with pytest.raises(llm_config.LLMConfigError):
        llm_config.ensure_configured()


def test_standalone_entrypoints_bootstrap_llm_config():
    """Each library CLI that can run standalone must (a) accept `--config`
    (highest-precedence per the loader contract) and (b) bootstrap via
    ensure_configured(args.config) before it reaches the model — those paths never
    route through job_migrate.apply_cfg_to_modules(), so MODEL would be None."""
    missing = []
    for name in ("agent_migrate.py", "fuse_package_analyzer.py",
                 "deep_analyze_notebook.py", "migrate_validate.py"):
        src = (_SCRIPTS / name).read_text(encoding="utf-8")
        if '"--config"' not in src or "ensure_configured(args.config)" not in src:
            missing.append(name)
    assert missing == [], f"standalone entrypoints missing --config/ensure_configured wiring: {missing}"


@pytest.mark.parametrize("entry,extra", [
    ("deep_analyze_notebook.py", ["some_notebook.ipynb"]),  # needs work-arg to reach the loader
    ("migrate_validate.py", ["--cluster", "c"]),
    ("fuse_package_analyzer.py", ["--packages", "pkg"]),
    ("agent_migrate.py", ["--cluster", "c"]),
])
def test_standalone_entrypoints_accept_config_and_route_to_loader(entry, extra, tmp_path):
    """PR#146 review (o-vazunnavar): pin the REAL entrypoint wiring, not a toy parser.
    Subprocess each of the four standalone commands with `--config <nonexistent>` and
    require that it: (a) accepts `--config` (never argparse 'unrecognized arguments:
    --config'), (b) imports cleanly under direct-script execution (no
    ModuleNotFoundError), and (c) forwards the path to the shared loader, which fails
    loudly with the missing-config error naming our path. HOME/cwd are a clean tmp dir
    so no fallback `aidp_migrator.yaml` is found (deterministic)."""
    import subprocess
    bad = str(tmp_path / "nope.yaml")   # nonexistent
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "AIDP_CONFIG_PATH")}
    env["HOME"] = str(tmp_path)         # no ~/aidp_migrator.yaml here
    r = subprocess.run(
        [sys.executable, str(_SCRIPTS / entry), *extra, "--config", bad],
        cwd=str(tmp_path), capture_output=True, text=True, timeout=180, env=env)
    out = r.stdout + r.stderr
    assert "unrecognized arguments: --config" not in out, f"{entry} rejected --config: {out[-500:]}"
    assert "ModuleNotFoundError" not in out, f"{entry} bootstrap import failed: {out[-500:]}"
    assert bad in out or "No customer configuration file found" in out, \
        f"{entry} did not forward --config to the shared loader: {out[-500:]}"


def test_ensure_configured_imports_aidp_config_under_direct_script_execution(tmp_path):
    """Regression: `python scripts/foo.py` puts only scripts/ on sys.path (not the
    repo root where aidp_config lives). ensure_configured() must add the repo root
    and import aidp_config — NOT ModuleNotFoundError before config loads."""
    import subprocess
    # Simulate direct-script execution: cwd is elsewhere, only scripts/ is added,
    # PYTHONPATH stripped so the repo root is not injected by the environment.
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(_SCRIPTS)!r})\n"
        "import llm_config\n"
        "try:\n"
        "    llm_config.ensure_configured()\n"
        "except ModuleNotFoundError as e:\n"
        "    print('MODNOTFOUND:' + str(e)); sys.exit(3)\n"
        "except Exception as e:\n"
        "    print('PAST_IMPORT:' + type(e).__name__); sys.exit(0)\n"
        "print('CONFIGURED'); sys.exit(0)\n"
    )
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    r = subprocess.run([sys.executable, "-c", code], cwd=str(tmp_path),
                       capture_output=True, text=True, env=env)
    assert "MODNOTFOUND" not in r.stdout, f"aidp_config import failed: {r.stdout}{r.stderr}"
    assert r.returncode == 0, f"{r.stdout}{r.stderr}"


def test_deep_analyze_no_args_shows_help_not_config_error(tmp_path):
    """PR#146 review (o-vazunnavar): deep_analyze_notebook.py with no notebook/batch
    must print help WITHOUT triggering config discovery. Regression: it called
    ensure_configured() before the arg check, so running with no args failed on a
    missing config instead of showing help. Config is only needed once there is work."""
    import subprocess
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "AIDP_CONFIG_PATH")}
    env["HOME"] = str(tmp_path)   # no ~/aidp_migrator.yaml
    r = subprocess.run([sys.executable, str(_SCRIPTS / "deep_analyze_notebook.py")],
                       cwd=str(tmp_path), capture_output=True, text=True, timeout=120, env=env)
    out = r.stdout + r.stderr
    assert r.returncode == 0, f"no-args should exit 0 (help), got {r.returncode}: {out[-400:]}"
    assert "usage:" in out.lower(), f"no-args should print help/usage: {out[-400:]}"
    assert "No customer configuration file found" not in out, \
        "no-args must NOT trigger config discovery (the regression)"


def test_user_visible_output_not_pinned_to_a_model_family():
    """After externalizing the model, operator-facing output/reports must not hard-
    assert a model family (would be factually wrong once the YAML model changes)."""
    banned = {
        "job_migrate.py": ["cost estimate = tokens × Opus rate"],
        # PR#146 review (o-vazunnavar): operator-facing AND docstring model-name
        # claims removed in favour of the configured model / generic "configured LLM"
        # — pin them gone (a model swap must not make these false).
        "deep_analyze_notebook.py": ['"Model: Claude Opus 5', "Uses Claude Opus 5"],
        "migrate_validate.py": ["Sonnet 1-2, Opus 3-5", "Sonnet x2, then Opus x3",
                                "Migrating with Claude Opus"],
        "agent_migrate.py": ["Analyzing with Claude Opus", "Migrating with Claude Opus",
                             "fast model (Sonnet)", "ANALYZE with Claude Opus",
                             "MIGRATE with Claude Opus"],
        "context_tools.py": ["Claude Opus make better", "Context Tools for Opus"],
        "context_compactor.py": ["Opus summarization", "if Opus call fails"],
    }
    offenders = []
    for name, needles in banned.items():
        src = (_SCRIPTS / name).read_text(encoding="utf-8")
        offenders += [f"{name}: {n}" for n in needles if n in src]
    assert offenders == [], f"stale model-specific operator output: {offenders}"


def test_general_analysis_calls_read_the_max_tokens_constant():
    """The GENERAL single-shot analysis calls read `llm_config.MAX_TOKENS` (a fixed
    internal constant since PR#146, no longer configurable). Purpose-specific small
    calls keep their own small budgets, and the primary tool-use loop passes its own
    large per-call budgets — that scope is documented, not pinned here."""
    missing = []
    for name in ("deep_analyze_notebook.py", "migrate_validate.py"):
        if "llm_config.MAX_TOKENS" not in (_SCRIPTS / name).read_text(encoding="utf-8"):
            missing.append(name)
    assert missing == [], f"general analysis calls not reading configured max_tokens: {missing}"


def test_behaviour_params_default_when_omitted():
    # provider+model given; thinking/max_tokens omitted -> safe defaults.
    llm_config.configure(_cfg(provider="anthropic", model="claude-opus-5-5"))
    assert llm_config.MODEL == "claude-opus-5-5"
    assert llm_config.THINKING == {"type": "adaptive"}
    assert llm_config.MAX_TOKENS == 16000


def test_thinking_kwargs_splat_and_omit():
    llm_config.THINKING = {"type": "adaptive"}
    assert llm_config.thinking_kwargs() == {"thinking": {"type": "adaptive"}}
    llm_config.THINKING = None                                # a model with no thinking
    assert llm_config.thinking_kwargs() == {}                 # param omitted entirely


def test_no_call_site_hardcodes_the_model():
    """Every LLM call site must read llm_config.MODEL, never a literal — otherwise
    swapping the model from YAML would silently miss that site."""
    import re
    # catch ANY hardcoded model string literal (model="..."/model='...'), not just one
    # value — a call site must use llm_config.MODEL. (A literal of any model id defeats
    # YAML model switching.)
    offenders = []
    for name in ("agent_migrate.py", "context_compactor.py", "fuse_package_analyzer.py",
                 "deep_analyze_notebook.py", "migrate_validate.py"):
        src = (_SCRIPTS / name).read_text(encoding="utf-8")
        if re.search(r'\bmodel\s*=\s*[\'"]', src):
            offenders.append(name)
    assert offenders == [], f"hardcoded model string literal still present in: {offenders}"
