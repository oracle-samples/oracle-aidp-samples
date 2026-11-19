"""Unit tests for :mod:`oracle_ai_data_platform_fusion_autopilot.commands.resolution_prompt`.

Covers the three operator-interaction modes:

* ``--non-interactive`` auto-picks the first candidate.
* Interactive: operator types a number → that candidate is chosen.
* Interactive: operator presses Enter → default (first) is chosen.
* Invalid input loops the prompt until a valid one is entered.
"""

from __future__ import annotations

from io import StringIO

import pytest
from rich.console import Console

from oracle_ai_data_platform_fusion_autopilot.commands.resolution_prompt import (
    PromptResult,
    prompt_multi_match,
)


def _muted_console() -> Console:
    return Console(file=StringIO(), force_terminal=False)


class TestNonInteractive:
    def test_auto_picks_first_candidate(self) -> None:
        result = prompt_multi_match(
            variation_point_name="invoice_currency_code",
            kind="columnAliases",
            matched=["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"],
            non_interactive=True,
            console=_muted_console(),
        )
        assert result == PromptResult(
            chosen="ApInvoicesInvoiceCurrencyCode",
            mechanism="non_interactive",
        )


class TestInteractive:
    def test_default_choice_on_empty_input(self) -> None:
        result = prompt_multi_match(
            variation_point_name="invoice_currency_code",
            kind="columnAliases",
            matched=["First", "Second"],
            non_interactive=False,
            console=_muted_console(),
            input_fn=lambda _: "",  # operator pressed Enter
        )
        assert result == PromptResult(chosen="First", mechanism="terminal_prompt")

    def test_explicit_choice(self) -> None:
        result = prompt_multi_match(
            variation_point_name="cancelled_status",
            kind="semanticVariants",
            matched=["cancelled_date", "cancelled_flag"],
            non_interactive=False,
            console=_muted_console(),
            input_fn=lambda _: "2",
        )
        assert result == PromptResult(
            chosen="cancelled_flag", mechanism="terminal_prompt"
        )

    def test_invalid_input_loops_until_valid(self) -> None:
        responses = iter(["banana", "99", "0", "1"])
        result = prompt_multi_match(
            variation_point_name="invoice_currency_code",
            kind="columnAliases",
            matched=["First", "Second"],
            non_interactive=False,
            console=_muted_console(),
            input_fn=lambda _: next(responses),
        )
        assert result.chosen == "First"
        assert result.mechanism == "terminal_prompt"


class TestContractEnforcement:
    def test_empty_matched_raises(self) -> None:
        with pytest.raises(ValueError, match="MultiMatch.matched"):
            prompt_multi_match(
                variation_point_name="x",
                kind="columnAliases",
                matched=[],
                non_interactive=True,
                console=_muted_console(),
            )
