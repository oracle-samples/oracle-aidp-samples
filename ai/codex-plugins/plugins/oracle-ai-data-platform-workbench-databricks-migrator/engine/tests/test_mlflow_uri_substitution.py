"""MLflow URI sourcing — Phase 2 invariants (credential-store runtime fetch).

History of this guard:
  Session 1: URI moved from scattered literals to scripts/constants.py.
  Phase 1 (PR#55): constants.py deleted; URI in customer YAML; prompts carried
    a `<MLFLOW_TRACKING_URI>` placeholder substituted at main() startup.
  Phase 2 (PR#59): placeholder mechanism DELETED. Migrated cells now fetch the
    URI at cluster runtime from the AIDP Credential Store:
        aidputils.secrets.get(AIDP_CRED_SECRETS_NAME, key=AIDP_CRED_MLFLOW_KEY)
    with defaults 'migration_tool_secrets' / 'mlflow-tracking-uri'. The env
    vars are exported by apply_cfg_to_modules() from the customer YAML's
    secret ref ({from, name, key}) and seeded onto the kernel by
    build_env_bootstrap_snippet().

This file pins the Phase-2 shape:
  1. No literal MLflow host fragment anywhere in source.
  2. The old placeholder mechanism must NOT come back (a regression to
     laptop-side substitution would silently bypass the credential store).
  3. Both prompts instruct the runtime fetch, reading BOTH env knobs (name
     and key) so the YAML ref's `name:` and `key:` fields are honored.
  4. apply_cfg_to_modules exports AIDP_CRED_SECRETS_NAME and
     AIDP_CRED_MLFLOW_KEY, and the kernel bootstrap forwards them.
"""

from __future__ import annotations

import pathlib
import re
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
AGENT_MIGRATE = REPO / "scripts" / "agent_migrate.py"
JOB_MIGRATE = REPO / "scripts" / "job_migrate.py"

_URL_HOST_FRAGMENT = "mlflow.customer.example"
_OLD_PLACEHOLDER = "<MLFLOW_TRACKING_URI>"


class TestMlflowUrlNotInSource(unittest.TestCase):
    """The URL literal must live in the customer credential store, not source."""

    def test_zero_url_literals_in_agent_migrate(self):
        src = AGENT_MIGRATE.read_text(encoding="utf-8")
        self.assertNotIn(_URL_HOST_FRAGMENT, src,
                         "MLflow host fragment leaked into agent_migrate.py — "
                         "URL must live in the AIDP Credential Store, not source.")

    def test_zero_url_literals_in_job_migrate(self):
        src = JOB_MIGRATE.read_text(encoding="utf-8")
        self.assertNotIn(_URL_HOST_FRAGMENT, src,
                         "MLflow host fragment leaked into job_migrate.py — "
                         "URL must live in the AIDP Credential Store, not source.")


class TestPlaceholderMechanismRemoved(unittest.TestCase):
    """Phase 2 deleted the laptop-side placeholder substitution. If the
    placeholder or its .replace() wiring reappears, someone is reintroducing
    laptop-injected plaintext — the exact thing Phase 2 eliminated."""

    def test_no_placeholder_in_either_prompt_file(self):
        self.assertNotIn(_OLD_PLACEHOLDER, AGENT_MIGRATE.read_text(encoding="utf-8"))
        self.assertNotIn(_OLD_PLACEHOLDER, JOB_MIGRATE.read_text(encoding="utf-8"))

    def test_no_substitution_wiring_left(self):
        src = JOB_MIGRATE.read_text(encoding="utf-8")
        self.assertNotIn('CELL_MIGRATE_PROMPT.replace("<MLFLOW_TRACKING_URI>"', src)
        self.assertNotIn("_am_runtime._MLFLOW_TRACKING_URI", src)


class TestRuntimeFetchInPrompts(unittest.TestCase):
    """Both prompts must instruct the credential-store runtime fetch, reading
    the credential NAME and the KEY from env (with the documented defaults) so
    the YAML ref's `name:` and `key:` fields both work."""

    _NAME_KNOB = "os.environ.get('AIDP_CRED_SECRETS_NAME', 'migration_tool_secrets')"
    _KEY_KNOB = "os.environ.get('AIDP_CRED_MLFLOW_KEY', 'mlflow-tracking-uri')"
    _FETCH_RE = re.compile(
        r"mlflow\.set_tracking_uri\(aidputils\.secrets\.get\(_SECRETS,\s*key=_MLFLOW_KEY\)"
    )

    def test_cell_migrate_prompt_fetch(self):
        src = JOB_MIGRATE.read_text(encoding="utf-8")
        self.assertIn(self._NAME_KNOB, src)
        self.assertIn(self._KEY_KNOB, src)
        self.assertRegex(src, self._FETCH_RE)

    def test_fix_prompt_fetch(self):
        src = AGENT_MIGRATE.read_text(encoding="utf-8")
        self.assertIn(self._NAME_KNOB, src)
        self.assertIn(self._KEY_KNOB, src)
        self.assertRegex(src, self._FETCH_RE)


class TestEnvKnobsWired(unittest.TestCase):
    """apply_cfg_to_modules must export both knobs from the YAML ref, and the
    kernel bootstrap must forward them (behavioral coverage of the export
    values lives in tests/config/test_apply_cfg_secrets_and_creds.py)."""

    def test_apply_cfg_exports_both_knobs(self):
        src = JOB_MIGRATE.read_text(encoding="utf-8")
        self.assertIn('os.environ["AIDP_CRED_SECRETS_NAME"] = _ref_name', src)
        self.assertIn('os.environ["AIDP_CRED_MLFLOW_KEY"] = _mlflow_key', src)

    def test_bootstrap_forwards_both_knobs(self):
        src = JOB_MIGRATE.read_text(encoding="utf-8")
        self.assertIn('envs["AIDP_CRED_SECRETS_NAME"] = cred_secrets_name', src)
        self.assertIn('envs["AIDP_CRED_MLFLOW_KEY"] = mlflow_key', src)

    def test_key_knob_cleared_between_apply_cfg_calls(self):
        """AIDP_CRED_MLFLOW_KEY must be in apply_cfg's pop-before-set list so
        a lean config after a full config cannot inherit the stale key name."""
        src = JOB_MIGRATE.read_text(encoding="utf-8")
        pop_block = src[src.find("AIDP_CRED_OCI_KEY_USER"):]
        pop_block = pop_block[:pop_block.find("os.environ.pop") + 40]
        self.assertIn("AIDP_CRED_MLFLOW_KEY", pop_block,
                      "AIDP_CRED_MLFLOW_KEY missing from the pop-before-set list")


if __name__ == "__main__":
    unittest.main()
