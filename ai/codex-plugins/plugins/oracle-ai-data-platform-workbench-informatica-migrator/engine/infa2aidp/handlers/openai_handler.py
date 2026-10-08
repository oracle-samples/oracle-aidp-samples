"""OpenAI integration for complex transformation conversion.

The sibling of ``codellama_handler.LLMHandler`` (Anthropic), for the Codex
build of this plugin. Converts Informatica expressions, stored procedures and
SQL to PySpark through the OpenAI API. Requires ``OPENAI_API_KEY``; the
default model is overridable via ``OPENAI_MODEL``.

Why a second handler rather than a provider flag inside one class: the two
SDKs differ in how a response is unwrapped, how retries surface, and which
errors are retryable. Branching on a provider string inside one method made
both paths harder to read and neither easier to test. The two handlers share
the interface the pipeline depends on -- ``is_available()`` and
``_call(prompt)`` -- and nothing else.

**The LLM path is optional.** This tool's coverage comes from its
deterministic compiler: 36 transformation types and 72 expression functions
convert with no model involved. Without ``OPENAI_API_KEY`` the migrator runs
rule-based only, which is a supported configuration and not a degraded one.
"""

import hashlib
import os

try:
    import openai as _openai_mod
except ImportError:                                         # pragma: no cover
    _openai_mod = None

from .. import config as _cfg

DEFAULT_OPENAI_MODEL = getattr(_cfg, "OPENAI_MODEL", "gpt-4o")

_SYSTEM_PROMPT = (
    "You are an expert Informatica PowerCenter to PySpark migration specialist. "
    "Convert the given Informatica expression/code to equivalent PySpark code. "
    "Return ONLY the PySpark code, no explanations."
)


class OpenAIHandler:
    """Converts Informatica expressions to PySpark using the OpenAI API."""

    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 3,
        model: str = DEFAULT_OPENAI_MODEL,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.model = model
        self._cache: dict[str, str] = {}
        self._client = None
        self._init_client()

    def _init_client(self):
        """Create the OpenAI client from OPENAI_API_KEY."""
        if _openai_mod is None:
            return
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key:
            self._client = _openai_mod.OpenAI(
                api_key=api_key, timeout=self.timeout,
                max_retries=self.max_retries,
            )

    def is_available(self) -> bool:
        return self._client is not None

    # ------------------------------------------------------------------

    def _call(self, prompt: str) -> str:
        """Convert one prompt. Cached, because the pipeline re-asks.

        A migration converts many near-identical expressions; the fixer may
        also re-send a prompt it has already sent. Caching on the prompt
        keeps that from being billed twice.
        """
        if not self.is_available():
            raise RuntimeError(
                "OpenAI is not configured: set OPENAI_API_KEY, or run without "
                "--use-llm for rule-based conversion only."
            )
        key = hashlib.sha256(f"{self.model}\x00{prompt}".encode()).hexdigest()
        if key in self._cache:
            return self._cache[key]

        response = self._client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        text = self._extract_text(response)
        self._cache[key] = text
        return text

    def generate(
        self,
        prompt: str,
        system: str | None = None,
        max_tokens: int | None = None,
        timeout: int | None = None,
    ) -> str:
        """A long-form generation: a whole notebook, or a validation verdict.

        Distinct from ``_call`` in two ways that matter. The caller supplies
        the system prompt, because notebook generation and validation use
        different ones and the generic expression prompt is wrong for both.
        And the result is NOT cached: these prompts are large, near-unique
        per mapping, and the retry loop above this deliberately re-asks after
        a rate limit -- caching would serve a stale answer to a retry that
        exists precisely because the first answer never arrived.
        """
        if not self.is_available():
            raise RuntimeError(
                "OpenAI is not configured: set OPENAI_API_KEY, or run without "
                "--use-llm for rule-based conversion only."
            )
        client = self._client
        if timeout is not None:
            client = client.with_options(timeout=timeout)
        kwargs = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system or _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        }
        if max_tokens is not None:
            kwargs["max_completion_tokens"] = max_tokens
        return self._extract_text(client.chat.completions.create(**kwargs))

    @staticmethod
    def _extract_text(response) -> str:
        """The assistant text, defensively.

        A refusal or a length-truncated completion can leave ``content``
        None, which would otherwise propagate as a TypeError several frames
        away from the cause.
        """
        try:
            choice = response.choices[0]
        except (AttributeError, IndexError) as exc:
            raise RuntimeError(f"OpenAI returned no choices: {response!r}") from exc
        content = getattr(choice.message, "content", None)
        if not content:
            reason = getattr(choice, "finish_reason", "unknown")
            raise RuntimeError(
                f"OpenAI returned no content (finish_reason={reason!r}). "
                f"A refusal or a truncated completion cannot be used as code."
            )
        return _strip_code_fence(content.strip())


def _strip_code_fence(text: str) -> str:
    """Remove a ```python fence if the model wrapped its answer in one.

    The prompt asks for bare code, and the fence usually does not appear --
    but a fenced answer pasted into a notebook cell is a syntax error, so it
    is cheaper to strip it than to re-prompt.
    """
    if not text.startswith("```"):
        return text
    lines = text.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()
