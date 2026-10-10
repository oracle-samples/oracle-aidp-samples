"""Configuration loader for infa2aidp.

Loads settings from a .env file on import. Sources, in order of precedence:
  1. Environment variables (already set via export) -- never overwritten
  2. The file named by INFA2AIDP_ENV_FILE, if set (an explicit opt-in)
  3. ~/.infa2aidp/.env (user home)

A ``.env`` in the CURRENT DIRECTORY is deliberately not read
(SEC-AIDP-SAMPLES-INFA-H2). The CLI is run from inside customer export
bundles, and a ``.env`` planted in one used to set ``ANTHROPIC_BASE_URL`` /
``OPENAI_BASE_URL`` / ``HTTPS_PROXY`` / ``SSL_CERT_FILE`` for the whole
process -- the first LLM call then sent the operator's real API key, and the
customer's mapping metadata, wherever the bundle pointed, with nothing on
the terminal. One found there is now ignored with a WARNING; to use it, name
it: ``INFA2AIDP_ENV_FILE=./.env``.

A .env file may only set the keys this tool documents (``env.template``):
anything else is ignored with a WARNING naming the key, and the names that
decide where HTTP traffic goes or how it is trusted (``*_BASE_URL``,
``*_PROXY``, ``SSL_CERT_FILE``, ``REQUESTS_CA_BUNDLE``, ``OCI_CONFIG_FILE``,
``PYTHON*``...) are refused even from a trusted file. Export them in the
shell if you really mean them.

All settings have sensible defaults. Users only set what they need.
"""

import os
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Names an explicit .env file: the only way a .env outside ~/.infa2aidp is read.
ENV_FILE_VAR = "INFA2AIDP_ENV_FILE"

# Keys a .env file may set: everything env.template documents, by name or
# by this tool's own prefixes. Documented knobs the SDKs and libraries also
# read from the environment are listed deliberately so a reviewer sees
# them; they are NOT the transport-steering names refused below.
ALLOWED_ENV_KEYS = frozenset({
    "ANTHROPIC_API_KEY", "CLAUDE_MODEL", "CLAUDE_MAX_TOKENS", "CLAUDE_TIMEOUT_SECONDS",
    "TARGET_SCORE", "MAX_ATTEMPTS", "BATCH_WORKERS",
    "MAX_CRITICAL", "MAX_WARNINGS", "MAX_ERRORS",
    "AUTO_RERUN", "MAX_RERUNS", "RULE_BASED_FALLBACK", "TARGET_CATALOG_TYPE",
    "OCI_PROFILE",
})
_ALLOWED_ENV_PREFIXES = ("INFA_", "AIDP_", "LLM_", "CLAUDE_", "OPENAI_")

# Refused from ANY .env file, allow-listed or not. Each of these moves the
# process's HTTP traffic (or its trust in it, or its code) somewhere else.
_REFUSED_ENV_KEY = re.compile(
    r"(_BASE_URL$|_API_BASE$|_PROXY$|^NO_PROXY$|^SSL_CERT_|^REQUESTS_CA_BUNDLE$|^CURL_CA_BUNDLE$"
    r"|^OCI_CONFIG_FILE$|^OCI_CLI_|^PYTHON|^PATH$|^LD_|^DYLD_|^NODE_OPTIONS$)",
    re.IGNORECASE,
)


def env_key_allowed(key: str) -> bool:
    """Whether a .env file may set *key*. The refusal list wins: an
    ``OPENAI_BASE_URL`` carries an allowed prefix and is still refused."""
    if not key or _REFUSED_ENV_KEY.search(key):
        return False
    return key in ALLOWED_ENV_KEYS or key.startswith(_ALLOWED_ENV_PREFIXES)


def _env_search_paths() -> "list[Path]":
    """The files read, in order: the explicit one (if any), then the home one.
    Computed per call -- never at import -- so a test or a long-lived
    process sees the current environment."""
    paths = []
    explicit = os.environ.get(ENV_FILE_VAR, "").strip()
    if explicit:
        paths.append(Path(explicit).expanduser())
    paths.append(Path.home() / ".infa2aidp" / ".env")
    return paths


def _parse_env_file(env_path: Path):
    """Yield ``(key, value)`` for every ``KEY=value`` line. No external deps."""
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            yield key.strip(), value.strip().strip("'\"")  # Remove quotes


def _is_same_file(a: Path, b: Path) -> bool:
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return False


def _load_env_file():
    """Load the explicit and/or home .env (see the module docstring).

    Returns the path of the first file loaded, or None. Every file found is
    loaded, the earlier one winning on a repeated key because a value
    already in the environment is never overwritten. A ``.env`` in the
    current directory is reported and skipped unless it IS the explicit
    file.
    """
    paths = _env_search_paths()
    cwd_env = Path.cwd() / ".env"
    if cwd_env.is_file() and not any(_is_same_file(cwd_env, p) for p in paths):
        # WARNING, not INFO: emitted before the CLI configures logging, and
        # logging's last-resort handler shows WARNING and above.
        logger.warning(
            "Ignoring %s -- a .env in the current directory is not read, because the "
            "current directory is often a customer export bundle. To use it, set "
            "%s=%s", cwd_env, ENV_FILE_VAR, cwd_env,
        )

    loaded_from = None
    for env_path in paths:
        if not env_path.is_file():
            if env_path is paths[0] and os.environ.get(ENV_FILE_VAR, "").strip():
                logger.warning("%s names %s, which is not a file -- nothing loaded from it",
                               ENV_FILE_VAR, env_path)
            continue
        logger.info("Loading config from %s", env_path)
        refused = []
        for key, value in _parse_env_file(env_path):
            if not env_key_allowed(key):
                refused.append(key)
                continue
            # Don't overwrite existing env vars
            if key not in os.environ:
                os.environ[key] = value
        if refused:
            # Names only -- a refused value may be a secret.
            logger.warning(
                "%s: ignored %d setting(s) this tool does not read, or does not accept from "
                "a .env file: %s. Export them in the shell if they are intended.",
                env_path, len(refused), ", ".join(sorted(set(refused))),
            )
        loaded_from = loaded_from or str(env_path)
    return loaded_from


def _get(key: str, default: str = "") -> str:
    """Get a config value from environment."""
    return os.environ.get(key, default)


def _get_int(key: str, default: int = 0) -> int:
    """Get an integer config value."""
    val = os.environ.get(key, "")
    try:
        return int(val) if val else default
    except ValueError:
        return default


# Load .env on import
_env_file = _load_env_file()


# ── LLM Provider Settings ──
#
# The LLM path is OPTIONAL. This tool's coverage comes from its deterministic
# compiler -- 36 transformation types and 72 expression functions convert with
# no model involved -- so running with no provider configured is a supported
# configuration, not a degraded one. A provider only widens what the
# rule-based path cannot reach.
#
# This is the Codex build, so OpenAI is the default provider. The Anthropic
# path is kept rather than removed: the engine is shared with the Claude Code
# build of this plugin, and deleting it here would fork the engine.
#
# LLM_PROVIDER picks between them: "openai" (default) or "anthropic". An
# explicit value is honoured even if the other provider's key is also set,
# because a machine with both keys exported should not get a provider by
# accident of which one was read first.

LLM_PROVIDER = _get("LLM_PROVIDER", "openai").strip().lower()

OPENAI_API_KEY = _get("OPENAI_API_KEY")
OPENAI_MODEL = _get("OPENAI_MODEL", "gpt-4o")

ANTHROPIC_API_KEY = _get("ANTHROPIC_API_KEY")
CLAUDE_MODEL = _get("CLAUDE_MODEL", "claude-opus-5")


def llm_provider() -> str:
    """The provider to use, after falling back to whichever key is present.

    An unset LLM_PROVIDER with only one key exported should just work; an
    explicit LLM_PROVIDER should win regardless of which keys exist, so a
    misconfiguration surfaces as "no key for the provider you asked for"
    rather than silently using the other one.
    """
    explicit = _get("LLM_PROVIDER")
    if explicit:
        return explicit.strip().lower()
    if OPENAI_API_KEY:
        return "openai"
    if ANTHROPIC_API_KEY:
        return "anthropic"
    return "openai"


def llm_status_line() -> str:
    """One line describing the LLM configuration, for `infa2aidp version`.

    Lives here rather than in cli.py because it is config knowledge, and
    cli.py is a thin dispatcher that the release gate holds under 500 lines.
    """
    provider = llm_provider()
    if not llm_api_key(provider):
        return f"LLM: no key for provider {provider!r} -- rule-based only"
    model = OPENAI_MODEL if provider == "openai" else CLAUDE_MODEL
    return f"LLM: configured (provider: {provider}, model: {model})"


def llm_api_key(provider: str | None = None) -> str | None:
    """The key for the chosen provider, or None if it is not configured."""
    provider = (provider or llm_provider()).strip().lower()
    return OPENAI_API_KEY if provider == "openai" else ANTHROPIC_API_KEY

# ── Migration Settings ──

TARGET_SCORE = _get_int("TARGET_SCORE", 90)
MAX_ATTEMPTS = _get_int("MAX_ATTEMPTS", 5)
BATCH_WORKERS = _get_int("BATCH_WORKERS", 5)

# ── Acceptance Criteria (ALL must be met) ──

MAX_CRITICAL = _get_int("MAX_CRITICAL", 0)      # Max critical issues allowed (0 = zero tolerance)
MAX_WARNINGS = _get_int("MAX_WARNINGS", -1)      # Max warnings allowed (-1 = unlimited)
MAX_ERRORS = _get_int("MAX_ERRORS", 0)            # Max errors allowed

# ── Batch Re-run Strategy ──

AUTO_RERUN = _get("AUTO_RERUN", "false").lower() in ("true", "1", "yes")
MAX_RERUNS = _get_int("MAX_RERUNS", 3)            # Max re-run cycles for batch

# ── AIDP Deploy (OCI SDK authentication) ──

AIDP_REGION = _get("AIDP_REGION")
AIDP_INSTANCE_ID = _get("AIDP_INSTANCE_ID")
AIDP_WORKSPACE_KEY = _get("AIDP_WORKSPACE_KEY")
OCI_PROFILE = _get("OCI_PROFILE", "DEFAULT")
AIDP_WORKSPACE_PATH = _get("AIDP_WORKSPACE_PATH", "/Migrated")

# ── Migration Safety ──────────────────────────────────────────
# When --use-llm is requested but the LLM exhausts MAX_ATTEMPTS:
#   false (default) → ERROR OUT — no silent fallback. Forces user to
#                     review the failure and decide what to do.
#   true            → silently fall back to rule-based converters
#                     (legacy behavior — can produce semantically wrong
#                     notebooks for complex transforms like unconnected
#                     lookups)
# Rule-based mode is still fully available when --use-llm is omitted.
RULE_BASED_FALLBACK = _get("RULE_BASED_FALLBACK", "false").lower() in ("true", "1", "yes")

# ── Claude tuning ────────────────────────────────────────────
# Max output tokens per LLM call. Opus 5 supports up to 32000 -- safe as a
# default because the generator calls messages.stream(), not .create(); a
# non-streaming call this large would exceed Anthropic's non-streaming
# ceiling.
# Bump if the LLM truncates JSON mid-array on very large mappings.
CLAUDE_MAX_TOKENS = _get_int("CLAUDE_MAX_TOKENS", 32000)

# Provider-neutral aliases. The notebook generator's long-form call applies
# these whichever provider is in use, so on this Codex build the OpenAI call
# was reading a setting named CLAUDE_MAX_TOKENS. LLM_* is the name to use;
# the CLAUDE_* ones are still honoured so an existing .env keeps working.
LLM_MAX_TOKENS = _get_int("LLM_MAX_TOKENS", CLAUDE_MAX_TOKENS)
# HTTP timeout for each LLM call (seconds). Opus on big inputs can take a few minutes.
CLAUDE_TIMEOUT_SECONDS = _get_int("CLAUDE_TIMEOUT_SECONDS", 600)
LLM_TIMEOUT_SECONDS = _get_int("LLM_TIMEOUT_SECONDS", CLAUDE_TIMEOUT_SECONDS)

# ── Target catalog type ───────────
# Which infa2aidp.generators.write_strategies.WriteStrategy generated write
# cells use: "delta" (managed Delta, the default -- DeltaTable.forName /
# .merge) or "adw" (external ADW/ALH/ATP catalog -- Spark JDBC overwrite,
# with upserts staged then MERGEd on the database via python-oracledb).
# Always an explicit setting -- never inferred from a table name or
# connection string. Overridable per-run via `--target-catalog-type`.
TARGET_CATALOG_TYPE = _get("TARGET_CATALOG_TYPE", "delta")


def get_env_file_path() -> str:
    """Return the path of the loaded .env file, or suggested path."""
    if _env_file:
        return _env_file
    return str(Path.home() / ".infa2aidp" / ".env")


def create_default_env_file(path: str = None):
    """Create a template .env file at the given path."""
    if path is None:
        path = str(Path.home() / ".infa2aidp" / ".env")

    env_dir = os.path.dirname(path)
    if env_dir:
        os.makedirs(env_dir, exist_ok=True)

    template = """# infa2aidp Configuration
# Place this file at ~/.infa2aidp/.env. A .env in the current directory is
# NOT read; to use one elsewhere, set INFA2AIDP_ENV_FILE=/path/to/.env.

# ── LLM (Anthropic Claude) ──
# Required for LLM-assisted conversion. Without it, only rule-based conversion runs.
# OPENAI_API_KEY=sk-proj-your-key-here      (Codex build default)
# ANTHROPIC_API_KEY=sk-ant-api03-your-key-here

# Optional — override the default Claude model (Opus 5).
# CLAUDE_MODEL=claude-opus-5

# ── Validation Loop ──
TARGET_SCORE=90
MAX_ATTEMPTS=5

# ── Batch ──
BATCH_WORKERS=5

# ── AIDP Workspace ──
# AIDP_WORKSPACE_PATH=/Migrated
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(template)
    return path
