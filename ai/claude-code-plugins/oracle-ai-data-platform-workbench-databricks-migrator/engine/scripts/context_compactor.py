"""
context_compactor.py — Multi-tier context compaction for Claude API tool loops.

Tier 1 (always-on): Tool result truncation — saves full result to disk, returns
                     first 8000 chars with a truncation notice.
Tier 2 (~500K tokens): Keep first message + last 6 messages, drop middle.
Tier 3 (~850K tokens): LLM summarization (the configured model) — replace the
                        conversation with a dense summary. Falls back to Tier 2 if
                        the summarization call fails.
"""

import llm_config  # central model/provider config
import os
import tempfile

_THINKING_BLOCK_TYPES = ("thinking", "redacted_thinking")


def strip_thinking_blocks(messages: list) -> list:
    """Return a copy of ``messages`` without thinking / redacted_thinking blocks.

    A thinking block's signature is bound to the exact prefix it was produced
    under (system, tools, every earlier message). Any client-side compaction —
    dropping middle turns, or replaying the turns under a different system /
    no tools for a summary — breaks that prefix, and on enforced accounts the
    API rejects the replayed block with a 400. Text and tool_use blocks stay, so
    every tool_result still has its tool_use. The input list is not mutated.
    """
    stripped = []
    for message in messages:
        content = message.get("content")
        if message.get("role") != "assistant" or not isinstance(content, list):
            stripped.append(message)
            continue
        kept = [b for b in content
                if (b.get("type") if isinstance(b, dict) else getattr(b, "type", None))
                not in _THINKING_BLOCK_TYPES]
        stripped.append(message if len(kept) == len(content) else {**message, "content": kept})
    return stripped
import re
from typing import List, Optional

# The only filenames save_and_truncate ever produces: ``tool_<NNN>_<tool>.txt``
# (zero-padded ASCII counter, tool names are identifiers). get_saved_output
# accepts nothing else, so the model-supplied argument can never name a path
# (SEC-AIDP-SAMPLES-DBX-NEW-01). Anchored with ``\A``/``\Z`` and applied with
# ``fullmatch`` so a trailing newline cannot slip past ``$``; ``[0-9]`` rather
# than ``\d`` so non-ASCII decimal digits are refused as well.
SAVED_OUTPUT_NAME = re.compile(r"\Atool_[0-9]{3,}_[A-Za-z0-9_]+\.txt\Z")

# Result prefixes callers can test for. A policy refusal is final: the caller
# must not keep searching other compactors for the same filename.
REFUSED_PREFIX = "[context_compactor] Refused"
NOT_FOUND_PREFIX = "[context_compactor] File not found"


def validate_output_filename(filename) -> Optional[str]:
    """Return None if ``filename`` is a bare saved-output name, else the reason
    it must be refused. Checked before any filesystem access."""
    if not isinstance(filename, str) or not filename:
        return "filename must be a non-empty string"
    if "/" in filename or "\\" in filename or filename in (".", ".."):
        return "path separators and relative segments are not allowed"
    if os.path.basename(filename) != filename or os.path.isabs(filename):
        return "only a bare filename is allowed"
    if not SAVED_OUTPUT_NAME.fullmatch(filename):
        return "only tool output files named tool_<NNN>_<tool>.txt can be retrieved"
    return None


class ContextCompactor:
    """Multi-tier context compaction for a single Claude API tool-call session."""

    TIER2_THRESHOLD = 500_000   # tokens
    TIER3_THRESHOLD = 850_000   # tokens
    TRUNCATE_AT = 8_000         # chars kept from each tool result

    def __init__(self, call_id: str, log_fn=None, record_usage=None):
        self._call_id = call_id
        self._log = log_fn if log_fn is not None else print
        # Called with the Tier-3 response's `usage` so the summary request is
        # billed in the caller's usage ledger (agent_migrate._record_usage).
        self._record_usage = record_usage
        self._tool_call_count = 0
        self._base_dir = None

        # AIDP_TMP_DIR can point at a per-migration directory created by a
        # different cluster user.  Context compaction is optional, so a stale
        # or inaccessible directory must not abort the entire migration.
        tmp_root = tempfile.gettempdir()
        per_user_root = None
        if hasattr(os, "getuid"):
            per_user_root = os.path.join(tmp_root, f"aidp_ctx_{os.getuid()}")
        roots = [
            os.environ.get("AIDP_TMP_DIR"),
            "/tmp",
            per_user_root,
            tmp_root,
        ]
        errors = []
        for root in dict.fromkeys(root for root in roots if root):
            candidate = os.path.join(root, "aidp_context", call_id)
            try:
                os.makedirs(candidate, exist_ok=True)
            except OSError as exc:
                errors.append(f"{candidate}: {exc}")
                continue
            self._base_dir = candidate
            break

        if self._base_dir is None:
            self._log(
                "[compaction] WARNING: context-file storage is unavailable; "
                "continuing without disk-backed compaction. " + "; ".join(errors)
            )

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def call_id(self) -> str:
        return self._call_id

    # ------------------------------------------------------------------
    # Tier 1 — Tool result truncation
    # ------------------------------------------------------------------

    def save_and_truncate(self, tool_name: str, result_str: str) -> str:
        """
        Save the full tool result to disk and return a truncated version with a
        notice pointing to the saved file.

        The on-disk filename uses a zero-padded counter so files sort in call
        order, e.g. ``tool_003_explore_path.txt``.
        """
        n = self._tool_call_count
        self._tool_call_count += 1

        filename = f"tool_{n:03d}_{tool_name}.txt"
        if self._base_dir is None:
            # Do not claim that output was saved when no writable directory
            # exists. Returning it intact preserves the migration's context.
            return result_str

        filepath = os.path.join(self._base_dir, filename)

        total_len = len(result_str)

        if total_len <= self.TRUNCATE_AT:
            # Short result — no need to save to disk, return as-is
            return result_str

        try:
            with open(filepath, "w", encoding="utf-8") as fh:
                fh.write(result_str)
        except OSError as exc:
            self._log(f"[compaction/tier1] WARNING: could not save tool output to {filepath}: {exc}")

        truncated = result_str[: self.TRUNCATE_AT]
        notice = (
            f"\n... [truncated, {total_len} chars total. "
            f"Full output saved to {filename} — use get_tool_output tool to retrieve]"
        )
        return truncated + notice

    # ------------------------------------------------------------------
    # Token estimation
    # ------------------------------------------------------------------

    @staticmethod
    def estimate_tokens(messages: list) -> int:
        """Fast token estimate: len(str(messages)) // 4."""
        return sum(len(str(m)) for m in messages) // 4

    # ------------------------------------------------------------------
    # Tier 2 — Drop middle messages
    # ------------------------------------------------------------------

    def _compact_tier2(self, messages: list) -> list:
        """Keep the first message and the last 6 messages, drop everything in between.

        Ensures the Anthropic API constraint that messages must alternate roles is
        maintained: if the last-6 block starts with a user turn (same role as the
        first message), insert a synthetic assistant separator.
        """
        if len(messages) <= 7:
            # Nothing meaningful to drop.
            return messages

        kept_first = messages[:1]
        kept_last = messages[-6:]
        dropped = len(messages) - 7
        self._log(
            f"[compaction/tier2] Dropping {dropped} middle messages "
            f"(keeping first 1 + last 6 of {len(messages)} total)."
        )

        # Anthropic API requires alternating roles. If both kept_first[-1] and
        # kept_last[0] are "user" turns, the splice would produce [user, user, ...]
        # which the API rejects. Insert a synthetic assistant separator.
        if kept_last[0].get("role") == "user":
            separator = [{"role": "assistant", "content": "[context trimmed — prior tool rounds omitted]"}]
            kept_last = separator + kept_last

        # The retained turns' thinking was produced with the dropped turns
        # present; replaying it after the splice is a 400 on enforced accounts.
        return strip_thinking_blocks(kept_first + kept_last)

    # ------------------------------------------------------------------
    # Tier 3 — LLM summarization
    # ------------------------------------------------------------------

    def _compact_tier3(self, messages: list, system: str = "") -> list:
        """
        Summarize the conversation with the configured LLM and replace messages with a
        3-message list: [original_prompt, assistant_summary, continue_prompt].

        Falls back to Tier 2 if the summarization call fails.
        """
        self._log(
            f"[compaction/tier3] Attempting LLM summarization "
            f"({self.estimate_tokens(messages):,} estimated tokens, {len(messages)} messages)."
        )

        try:
            import anthropic  # imported here to avoid hard dependency at module load

            client = anthropic.Anthropic()
            summary_prompt = (
                "Summarize this conversation for context compaction. This is a migration tool loop where "
                "Claude is migrating a Databricks notebook cell to OCI AIDP. Produce a dense summary covering:\n"
                "1. What cell is being migrated (notebook, cell index, what it does)\n"
                "2. What tools were called and what they returned (key findings only)\n"
                "3. What errors occurred and what fixes were attempted\n"
                "4. Current state: what code has been submitted, what's still failing\n"
                "5. What approaches have NOT been tried yet\n"
                "Be specific — include actual path names, error messages, variable names. Aim for ~2000 words."
            )

            response = client.messages.create(
                model=llm_config.MODEL,
                # Thinking is on by default for the Opus-5 family and counts
                # against max_tokens; 4096 could not hold it plus ~2000 words.
                max_tokens=llm_config.MAX_TOKENS,
                system=summary_prompt,
                # Different system and no tools than the loop's turns were
                # produced under: their thinking blocks would be rejected.
                messages=strip_thinking_blocks(messages),
            )
            if self._record_usage is not None:
                self._record_usage(response.usage)
            # Thinking blocks come first on this model family: read the text
            # blocks. Only end_turn is a complete summary — max_tokens cuts it
            # off and a refusal is HTTP 200 with partial text — and a partial
            # summary would silently drop conversation state, so anything else
            # takes the Tier 2 fallback below.
            summary_text = "".join(b.text for b in response.content
                                   if getattr(b, "type", "") == "text").strip()
            if response.stop_reason != "end_turn" or not summary_text:
                raise RuntimeError(f"no complete summary (stop_reason={response.stop_reason})")

        except Exception as exc:
            self._log(
                f"[compaction/tier3] LLM summarization failed ({exc}); falling back to Tier 2."
            )
            return self._compact_tier2(messages)

        self._log(
            f"[compaction/tier3] Summarization complete "
            f"({len(summary_text):,} chars). Replacing {len(messages)} messages with 3."
        )

        new_messages = [
            messages[0],  # original user prompt
            {
                "role": "assistant",
                "content": f"[CONTEXT COMPACTED]\n\n{summary_text}",
            },
            {
                "role": "user",
                "content": (
                    "Continue from the summary above. Note: prior tool call results are captured in "
                    "the summary — if you need fresh data from a path or table, re-call the relevant "
                    "tool (explore_path, run_on_cluster, etc.). Use submit_code when ready."
                ),
            },
        ]
        return new_messages

    # ------------------------------------------------------------------
    # maybe_compact — called before each API call
    # ------------------------------------------------------------------

    def maybe_compact(self, messages: list, system: str = "") -> list:
        """
        Inspect the estimated token count and apply the appropriate compaction
        tier, returning the (possibly modified) messages list.

        - Below 500K tokens  → return unchanged
        - 500K–850K tokens   → Tier 2 (drop middle)
        - Above 850K tokens  → Tier 3 (LLM summarization, fallback to Tier 2)
        """
        if not messages:
            return messages

        tokens = self.estimate_tokens(messages)

        if tokens >= self.TIER3_THRESHOLD:
            return self._compact_tier3(messages, system=system)

        if tokens >= self.TIER2_THRESHOLD:
            return self._compact_tier2(messages)

        return messages

    # ------------------------------------------------------------------
    # get_saved_output — retrieve a previously-saved tool result
    # ------------------------------------------------------------------

    def list_saved_outputs(self) -> List[str]:
        """Sorted names of the tool output files saved by this compactor."""
        try:
            names = os.listdir(self._base_dir)
        except OSError:
            return []
        return sorted(n for n in names if SAVED_OUTPUT_NAME.fullmatch(n))

    def get_saved_output(self, filename: str) -> str:
        """
        Read and return the contents of a file previously saved by
        ``save_and_truncate``.  Returns a descriptive error string if the file
        cannot be read.

        ``filename`` is model-supplied. Only a bare ``tool_<NNN>_<tool>.txt``
        name is accepted (see ``validate_output_filename``) and the resolved
        path must stay inside this compactor's directory; anything else is
        refused with a ``REFUSED_PREFIX`` string and logged. Error strings
        never echo host paths.
        """
        if self._base_dir is None:
            return "[context_compactor] Disk-backed context storage is unavailable."

        reason = validate_output_filename(filename)
        if reason is not None:
            self._log(f"[compaction] REFUSED get_saved_output({filename!r}): {reason}")
            return f"{REFUSED_PREFIX}: {reason}"

        # Defence in depth: resolve both sides (the base dir mixes separators
        # on Windows) and require the target to be a direct child of the base
        # dir, so a planted symlink cannot lead outside either.
        base_real = os.path.realpath(self._base_dir)
        filepath = os.path.realpath(os.path.join(base_real, filename))
        if os.path.dirname(filepath) != base_real:
            self._log(f"[compaction] REFUSED get_saved_output({filename!r}): resolves outside the tool output directory")
            return f"{REFUSED_PREFIX}: {filename} resolves outside the tool output directory"

        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                return fh.read()
        except FileNotFoundError:
            return (
                f"{NOT_FOUND_PREFIX}: {filename}. "
                f"Available files: {', '.join(self.list_saved_outputs()) or '(none)'}"
            )
        except OSError as exc:
            return f"[context_compactor] Could not read {filename}: {exc.strerror or exc.__class__.__name__}"
