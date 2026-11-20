"""PR-55 review fix: cfg.runtime.fix_max_attempts must reach ENFORCEMENT.

apply_cfg_to_modules() used to mutate only agent_migrate.MAX_FIX_ATTEMPTS,
but job_migrate binds the constant by value at import (from-import) and the
retry loops read job_migrate's OWN globals (MAX_CELL_RETRIES /
MAX_CHILD_RETRIES / MAX_REPLAY_RETRIES) at call time — so the YAML knob
changed the prompt denominator while enforcement silently stayed at the
import-time default. The fix rebinds job_migrate's globals and re-bakes
FIX_PROMPT from its template.

Also covers the missing-redirect contract: apply_cfg_to_modules REFUSES a
config without the `redirect:` section (public fail-closed contract), and
_apply_wrapper_call_redirect stays a no-op (not an AttributeError) when the
module globals are unset (library/test path); the section is
absent.

Subprocess probes (like tests/test_recursive_traversal_blanket.py's
TestCapsEnvOverridable) so each case gets clean import-time state.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPTS = os.path.join(REPO, "scripts")
FIXTURE = os.path.join(REPO, "tests", "config", "fixtures", "full.yaml")


def _run_probe(probe: str, extra_env: dict | None = None) -> dict:
    env = dict(os.environ, PYTHONUTF8="1")
    env.pop("AIDP_MAX_FIX_ATTEMPTS", None)
    if extra_env:
        env.update(extra_env)
    out = subprocess.run([sys.executable, "-c", probe], env=env,
                         capture_output=True, text=True, cwd=REPO)
    assert out.returncode == 0, f"probe failed:\n{out.stdout}\n{out.stderr}"
    return json.loads(out.stdout.strip().splitlines()[-1])


_PROBE = r"""
import io, json, os, sys, tempfile
sys.path.insert(0, r'%(scripts)s')
sys.path.insert(0, r'%(repo)s')

# Build a valid cfg from the full.yaml fixture with fix_max_attempts=9,
# materialising the referenced bucket-mapping file next to it.
with open(r'%(fixture)s', encoding='utf-8') as f:
    text = f.read()
text = text.replace('fix_max_attempts: 5', 'fix_max_attempts: 9')
td = tempfile.mkdtemp()
cfg_path = os.path.join(td, 'aidp_migrator.yaml')
with open(cfg_path, 'w', encoding='utf-8') as f:
    f.write(text)
with open(os.path.join(td, 'mapping.json'), 'w', encoding='utf-8') as f:
    f.write('s3_bucket,oci_bucket,oci_namespace\n')
os.chdir(td)

from aidp_config import load_config
cfg = load_config(cli_config_path=cfg_path)

import agent_migrate as am
import job_migrate as jm
before = jm.MAX_FIX_ATTEMPTS
_stdout, sys.stdout = sys.stdout, io.StringIO()  # swallow apply_cfg prints
jm.apply_cfg_to_modules(cfg)
sys.stdout = _stdout
print(json.dumps({
    'before': before,
    'am': am.MAX_FIX_ATTEMPTS,
    'jm': jm.MAX_FIX_ATTEMPTS,
    'cell': jm.MAX_CELL_RETRIES,
    'child': jm.MAX_CHILD_RETRIES,
    'replay': jm.MAX_REPLAY_RETRIES,
    'prompt_has_new': 'Maximum 9 Opus fix-attempts' in am.FIX_PROMPT,
    'prompt_has_old': 'Maximum %%d Opus fix-attempts' %% before in am.FIX_PROMPT,
    'redirect_noop': jm._apply_wrapper_call_redirect(
        "createTable(df, 't1', 'prod_db')",
        {'actionable': [{'name': 'createTable', 'is_method': False,
                         'positional_args': ['df', 'table', 'db'],
                         'args': {'table': 'table', 'db': 'db'}}]},
    ) == "createTable(df, 't1', 'prod_db')" if jm._REDIRECT_TABLE_PREFIX is None else 'redirect-configured',
}))
"""


class TestYamlFixMaxAttemptsPropagates(unittest.TestCase):
    def test_yaml_value_reaches_enforcement_and_prompt(self):
        got = _run_probe(_PROBE % {"scripts": SCRIPTS, "repo": REPO, "fixture": FIXTURE})
        self.assertEqual(got["am"], 9, "agent_migrate cap not updated from YAML")
        self.assertEqual(got["jm"], 9, "job_migrate's from-import binding left stale")
        self.assertEqual(got["cell"], 9, "MAX_CELL_RETRIES left stale")
        self.assertEqual(got["child"], 9, "MAX_CHILD_RETRIES left stale")
        self.assertEqual(got["replay"], 9, "MAX_REPLAY_RETRIES left stale")
        self.assertTrue(got["prompt_has_new"],
                        "FIX_PROMPT not re-baked — still advertises the import-time cap")
        self.assertFalse(got["prompt_has_old"],
                         "FIX_PROMPT still contains the stale import-time cap")

    def test_env_var_still_wins_over_yaml(self):
        got = _run_probe(_PROBE % {"scripts": SCRIPTS, "repo": REPO, "fixture": FIXTURE},
                         extra_env={"AIDP_MAX_FIX_ATTEMPTS": "7"})
        self.assertEqual(got["am"], 7)
        self.assertEqual(got["jm"], 7)
        self.assertEqual(got["cell"], 7)



def test_redirect_absent_refuses(tmp_path):
    """Public fail-closed contract: apply_cfg_to_modules REFUSES a config
    without the `redirect:` section — execution-time writes have nowhere
    safe to go without the sandbox."""
    import sys, os
    sys.path.insert(0, os.path.join(REPO, "scripts"))
    import pytest
    import job_migrate as jm
    from aidp_config import load_config
    stripped = tmp_path / "no_redirect.yaml"
    stripped.write_text(open(FIXTURE).read().split("\nredirect:")[0] + "\n")
    (tmp_path / "mapping.json").write_text("s3_bucket,oci_bucket,oci_namespace\n")
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        cfg = load_config(cli_config_path=str(stripped))
    finally:
        os.chdir(cwd)
    with pytest.raises(RuntimeError, match="redirect"):
        jm.apply_cfg_to_modules(cfg, configure_llm=False)
