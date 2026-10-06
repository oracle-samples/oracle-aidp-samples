"""Migration handlers: LLM integration, compatibility checking, and confidence scoring."""

from infa2aidp.handlers.codellama_handler import CodeLlamaHandler, LLMHandler
from infa2aidp.handlers.openai_handler import OpenAIHandler
from infa2aidp.handlers.compatibility_checker import CompatibilityChecker
from infa2aidp.handlers.confidence_scorer import (
    ConfidenceScorer,
    ConversionConfidence,
    ConversionScore,
)

def make_llm_handler(provider: str | None = None, **kwargs):
    """The handler for the configured provider.

    Returned by provider rather than chosen at the call site, so adding a
    provider does not mean editing the migrator. Both handlers expose the
    same two members the pipeline uses -- ``is_available()`` and
    ``_call(prompt)`` -- and nothing else is shared.

    Note this returns a handler even when no API key is set: the pipeline
    asks ``is_available()`` and falls back to rule-based conversion, which
    is a supported configuration. Raising here would turn "no key" into a
    failed migration instead of a deterministic one.
    """
    from .. import config as _cfg

    provider = (provider or _cfg.llm_provider()).strip().lower()
    if provider == "anthropic":
        return LLMHandler(**kwargs)
    if provider == "openai":
        return OpenAIHandler(**kwargs)
    raise ValueError(
        f"Unknown LLM_PROVIDER {provider!r}: expected 'openai' or 'anthropic'."
    )


__all__ = [
    "LLMHandler",
    "OpenAIHandler",
    "make_llm_handler",
    "CodeLlamaHandler",
    "CompatibilityChecker",
    "ConfidenceScorer",
    "ConversionConfidence",
    "ConversionScore",
]
