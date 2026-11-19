"""
Cell Context Accumulator
==========================
Carries forward context across cells within a notebook migration.
Prevents OpenAI model from re-exploring paths, re-reading notebooks, and
re-checking tables that were already verified for earlier cells.

Context is ordered newest-first so the most recent information
is at the top of the OpenAI model prompt.
"""

import json
from typing import Any, Dict, List, Optional


class CellContext:
    """Accumulated context that persists across cell migrations within a notebook."""

    def __init__(self):
        self.entries: List[str] = []                 # newest-first cell summaries
        self.tool_cache: Dict[str, str] = {}         # tool_name:json(input) -> result
        self.paths_checked: Dict[str, str] = {}      # path -> "EXISTS"/"NOT_FOUND"/"ERROR"
        self.tables_checked: Dict[str, str] = {}     # table/query -> result summary
        self.notebook_summaries: Dict[str, str] = {} # path -> summary (from OpenAI model)
        self._tool_call_count = 0
        self._cache_hit_count = 0

    def add_cell(self, cell_idx: int, total_cells: int, code_preview: str,
                 output_preview: str = "", status: str = "OK",
                 fixes: List[str] = None, child_info: str = None):
        """Record a completed cell. Called after each cell finishes."""
        entry = f"Cell {cell_idx}/{total_cells}: {status}"
        if child_info:
            entry += f" - {child_info}"
        if code_preview:
            entry += f" | {code_preview[:80]}"
        if output_preview:
            entry += f" | out: {output_preview[:60]}"
        if fixes:
            entry += f" | fixes: {'; '.join(fixes[:3])}"
        # Insert at beginning (newest first)
        self.entries.insert(0, entry)

    def add_tool_result(self, tool_name: str, tool_input: dict, result: str):
        """Cache a tool call result for deduplication."""
        key = self._cache_key(tool_name, tool_input)
        self.tool_cache[key] = result
        self._tool_call_count += 1

        # Extract structured info from known tools
        if tool_name == "explore_path":
            path = tool_input.get("path", "")
            if "EXISTS" in result:
                self.paths_checked[path] = "EXISTS"
            elif "NOT FOUND" in result:
                self.paths_checked[path] = "NOT_FOUND"
            else:
                self.paths_checked[path] = "ERROR"

        elif tool_name == "search_catalog":
            query = tool_input.get("query", "")
            if "No tables matching" in result:
                self.tables_checked[query] = "MISSING"
            else:
                # Extract just the table names
                self.tables_checked[query] = result[:120]

        elif tool_name == "suggest_oci_path":
            path = tool_input.get("path", "")
            if "Suggested OCI paths:" in result:
                # Store first suggestion
                lines = result.split("\n")
                for line in lines[1:]:
                    suggestion = line.strip()
                    if suggestion:
                        self.paths_checked[f"{path} -> {suggestion}"] = "SUGGESTED"
                        break

    def get_cached(self, tool_name: str, tool_input: dict) -> Optional[str]:
        """Return cached tool result or None if not cached."""
        key = self._cache_key(tool_name, tool_input)
        result = self.tool_cache.get(key)
        if result is not None:
            self._cache_hit_count += 1
        return result

    def add_notebook_summary(self, path: str, summary: str):
        """Cache a OpenAI model-generated notebook summary."""
        self.notebook_summaries[path] = summary

    def render(self, max_entries: int = 20) -> str:
        """Render the context as a text block for the OpenAI model prompt.
        Newest entries first. Truncates to max_entries."""
        parts = []

        if self.entries:
            parts.append("PRIOR CELLS (newest first):")
            for entry in self.entries[:max_entries]:
                parts.append(f"  {entry}")
            if len(self.entries) > max_entries:
                parts.append(f"  ... ({len(self.entries) - max_entries} older cells omitted)")

        if self.paths_checked:
            # Show most recent 15 paths
            items = list(self.paths_checked.items())[-15:]
            parts.append("\nVERIFIED PATHS:")
            for path, status in items:
                parts.append(f"  {status:12s} {path}")

        if self.tables_checked:
            items = list(self.tables_checked.items())[-10:]
            parts.append("\nVERIFIED TABLES:")
            for query, status in items:
                parts.append(f"  {query}: {status}")

        if self.notebook_summaries:
            parts.append("\nNOTEBOOK SUMMARIES:")
            for path, summary in list(self.notebook_summaries.items())[-5:]:
                parts.append(f"  {path}: {summary[:150]}")

        if self._cache_hit_count > 0:
            parts.append(f"\n(Tool cache: {self._cache_hit_count} hits / {self._tool_call_count} total calls)")

        return "\n".join(parts)

    def stats(self) -> str:
        """Return a one-line stats summary."""
        return (f"cells={len(self.entries)} paths={len(self.paths_checked)} "
                f"tables={len(self.tables_checked)} cache={self._cache_hit_count}/{self._tool_call_count}")

    @staticmethod
    def _cache_key(tool_name: str, tool_input: dict) -> str:
        return f"{tool_name}:{json.dumps(tool_input, sort_keys=True)}"
