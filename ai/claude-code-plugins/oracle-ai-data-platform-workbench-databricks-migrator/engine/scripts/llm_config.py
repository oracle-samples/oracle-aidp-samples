"""Central LLM provider/model configuration for the migration tool.

Single source of truth for WHICH model every LLM call uses. **The model and
provider come from the customer `aidp_config` YAML (`llm.provider` / `llm.model`)
— there is deliberately NO silent code default for them**: an absent/partial
`llm` section fails LOUDLY (consistent with the repo's "missing parameters raise
at the read point, never fabricate a default" rule). The default value lives in
the YAML template (`default_aidp_migrator.yaml`), not here, so swapping the model
is a YAML edit with no code change.

`model` is restricted (schema enum) to the validated Opus-5 family
(`claude-opus-5` / `claude-opus-5-5`) — switching between them is a YAML-only edit.

`thinking` and `max_tokens` are NOT configurable — they are FIXED internal constants
(`{"type":"adaptive"}` / 16000). They were removed from the config surface (PR#146
review): on the Opus-5 family `thinking: off` cannot disable adaptive thinking, and
`max_tokens` is not a true per-request cap (the primary tool-use loop passes its own
large per-call budgets; the small purpose-specific calls keep fixed budgets), so
accepting them from YAML would assert coverage the code does not have. The schema
(`additionalProperties:false`) rejects them at load. Re-add real, model-aware
validation — thinking mode + a genuine per-model output cap — only when a model that
needs it can be live-tested; that is a code+schema+test change, not a YAML edit.

SCOPE:
- `MODEL` — EVERY call site reads `llm_config.MODEL` (pinned by a test); fully central.
- `THINKING` (constant) — splatted (via `thinking_kwargs()`) into the primary tool-use
  migration requests only; the streaming fix path and aux tools send no thinking param.
- `MAX_TOKENS` (constant) — the general single-shot analysis calls read it; the primary
  tool-use loop and the small purpose-specific calls use their own budgets.

Multi-provider dispatch (a client factory) is likewise deferred to when a second
provider is available to TEST.

This is also the seam for future MULTI-PROVIDER support: `llm.provider` is recorded
now, but only "anthropic" is dispatched today. A provider-dispatch client factory
belongs here later, added when a second provider is actually available to TEST.

Call sites read `llm_config.MODEL` / `llm_config.thinking_kwargs()` at call time;
`configure()` (invoked from `job_migrate.apply_cfg_to_modules`) sets them from the
loaded config.
"""
from __future__ import annotations


class LLMConfigError(RuntimeError):
    """Raised when the required `llm` config is missing/incomplete."""


# No code default for provider/model — they MUST come from YAML (set by configure).
PROVIDER = None
MODEL = None
# Behaviour params are FIXED internal constants (NOT configurable — PR#146).
DEFAULT_THINKING = {"type": "adaptive"}
DEFAULT_MAX_TOKENS = 16000
THINKING = DEFAULT_THINKING           # dict passed to messages.create, or None
MAX_TOKENS = DEFAULT_MAX_TOKENS       # floor/default for the migration LLM calls


def thinking_kwargs():
    """`{'thinking': ...}` to splat into messages.create, or `{}` when disabled —
    so a model without extended thinking omits the param (passing thinking=None
    400s)."""
    return {"thinking": THINKING} if THINKING else {}


def configure(cfg) -> None:
    """Set PROVIDER/MODEL/THINKING/MAX_TOKENS from a loaded aidp_config.

    Requires `cfg.llm.provider` and `cfg.llm.model` — raises LLMConfigError if the
    `llm` section is absent or either is missing (no silent default; the value
    lives in the YAML). `thinking`/`max_tokens` are NOT read from cfg — they are
    fixed internal constants (adaptive / 16000); see the note at the assignment.
    Idempotent."""
    global PROVIDER, MODEL, THINKING, MAX_TOKENS
    llm = getattr(cfg, "llm", None)
    if llm is None:
        raise LLMConfigError(
            "aidp_config is missing the required `llm` section — set `llm.provider` "
            "and `llm.model` in your YAML (see default_aidp_migrator.yaml). There is "
            "no code default for the model.")
    provider = getattr(llm, "provider", None)
    model = getattr(llm, "model", None)
    if not provider or not model:
        raise LLMConfigError(
            "aidp_config `llm` requires both `provider` and `model` (no code "
            "default; set them in YAML).")
    # Only anthropic is dispatched today — every call site constructs
    # anthropic.Anthropic. Fail fast on an un-adapted provider rather than
    # accept `provider: openai` and 500 later inside the Anthropic client.
    # Adding a provider is a code+schema change (an adapter), not a YAML swap.
    if provider != "anthropic":
        raise LLMConfigError(
            f"aidp_config `llm.provider={provider!r}` is not supported — only "
            "'anthropic' is dispatched today. Adding a provider requires an "
            "adapter (code change), not a YAML edit.")
    PROVIDER = provider
    MODEL = model
    # thinking + output-budget are FIXED internal constants (adaptive / 16000), not
    # config knobs (PR#146 review): on the Opus-5 family `thinking: off` cannot
    # disable adaptive thinking, and max_tokens is not a true per-request cap — so
    # accepting them from YAML would be a surface that asserts coverage the code does
    # not have. Re-add real per-model validation when a model that needs it can be
    # live-tested. (The schema rejects `thinking`/`max_tokens`, so a stale config
    # fails loudly at load rather than silently here.)
    THINKING = DEFAULT_THINKING
    MAX_TOKENS = DEFAULT_MAX_TOKENS


def ensure_configured(cli_config_path=None) -> None:
    """Bootstrap MODEL/PROVIDER for STANDALONE entrypoints that don't route
    through `job_migrate.apply_cfg_to_modules()`.

    The main migration flow calls `configure()` directly, but the library
    modules' own CLIs (deep_analyze_notebook / migrate_validate /
    fuse_package_analyzer / agent_migrate) do not — without this they would pass
    `model=None` to the Anthropic client. This loads the standard config (honoring
    `--config` > `$AIDP_CONFIG_PATH` > `./aidp_migrator.yaml` > `~`) and configures.

    No-op if already configured (idempotent). Fail-loud (LLMConfigError) if no
    config / no `llm` block is found — deliberately no code default."""
    if MODEL is not None:
        return
    # Direct `python scripts/foo.py` puts only scripts/ on sys.path, not the repo
    # root where `aidp_config` lives (it is not an installed package). Add the repo
    # root so this import works under direct-script execution, matching what
    # job_migrate_from_workflow does at startup. Without this the standalone CLIs
    # would ModuleNotFoundError before any config loads.
    import os
    import sys
    _repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from aidp_config import load_config
    configure(load_config(cli_config_path=cli_config_path))
