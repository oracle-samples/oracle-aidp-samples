"""Terminal prompt for multi-match variation-point resolutions.

A :class:`MultiMatch` walker outcome means two or more candidates exist
on the tenant's bronze; the operator picks the right one. This is an
*interactive* decision unless ``--non-interactive`` is set, in which case the
prompt auto-picks the first candidate deterministically and records the weakest
approval evidence.

Output goes through Rich for consistency with the rest of the CLI's
terminal surface; input goes through stdlib ``input()`` so click's test
runner can drive the prompt deterministically.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from rich.console import Console
from rich.table import Table


@dataclass(frozen=True)
class PromptResult:
    """The outcome of a single multi-match prompt."""

    chosen: str
    """Logical id of the candidate the operator picked."""

    mechanism: str
    """Records how the choice was made — ``terminal_prompt`` or
    ``non_interactive``."""


def prompt_multi_match(
    *,
    variation_point_name: str,
    kind: str,
    matched: list[str],
    non_interactive: bool,
    console: Console | None = None,
    input_fn: Callable[[str], str] = input,
) -> PromptResult:
    """Prompt the operator to pick a candidate from ``matched``.

    Args:
        variation_point_name: e.g. ``"invoice_currency_code"``.
        kind: ``"columnAliases"`` or ``"semanticVariants"``.
        matched: priority-ordered list of matched candidate ids.
        non_interactive: when ``True``, auto-pick ``matched[0]`` and
            record ``mechanism: non_interactive``.
        console: Rich console for output (test injection).
        input_fn: stdlib-style input function (test injection). The
            prompt loops until the operator provides a valid choice.

    Returns:
        :class:`PromptResult` carrying the chosen candidate + the
        recorded mechanism. Bootstrap pins the chosen id and forwards
        the mechanism into the evidence snapshot's
        ``provenance.approvedBy.mechanism`` field.

    Raises:
        ValueError: when ``matched`` is empty (a walker bug — :class:`MultiMatch`
            by definition has ``len(matched) >= 2``; an empty list is
            never a legitimate prompt input).
    """
    if not matched:
        raise ValueError(
            "prompt_multi_match called with empty `matched`; this is a "
            "walker contract violation — MultiMatch.matched must have >= 2 entries."
        )

    if non_interactive:
        return PromptResult(chosen=matched[0], mechanism="non_interactive")

    console = console or Console()
    table = Table(
        title=f"Variation point {variation_point_name!r} matched multiple candidates",
        title_style="bold yellow",
    )
    table.add_column("#", justify="right")
    table.add_column("candidate")
    table.add_column("kind")
    for idx, candidate in enumerate(matched, start=1):
        table.add_row(str(idx), candidate, kind)
    console.print(table)
    console.print(
        f"[yellow]Pick the candidate to pin into "
        f"resolved.{kind[0:-1] if kind == 'columnAliases' else 'semantic'}."
        f"{variation_point_name}[/yellow]"
    )

    while True:
        raw = input_fn(f"Enter 1-{len(matched)} (default 1): ").strip()
        if raw == "":
            return PromptResult(chosen=matched[0], mechanism="terminal_prompt")
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(matched):
                return PromptResult(
                    chosen=matched[idx - 1], mechanism="terminal_prompt"
                )
        console.print(
            f"[red]Invalid choice {raw!r}; enter a number 1-{len(matched)}, "
            "or press Enter to accept default (1).[/red]"
        )


__all__ = ["PromptResult", "prompt_multi_match"]
