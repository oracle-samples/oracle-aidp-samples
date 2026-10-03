"""YAML-driven custom conversion rules engine.

Allows users to define their own transformation rules so they can
push conversion rates higher without modifying source code.  Rules
are loaded from a YAML file and applied in priority order before
the built-in converter runs.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class CustomRule:
    name: str
    description: str = ""
    match_type: str = ""       # "expression", "function", "pattern", "sql"
    match_pattern: str = ""    # Regex or exact match
    replacement: str = ""      # PySpark replacement template
    priority: int = 50         # Higher = applied first (0-100)
    enabled: bool = True


class CustomRuleEngine:
    """Load and apply custom conversion rules from YAML."""

    def __init__(self, rules_path: str = None):
        self.rules: list[CustomRule] = []
        if rules_path:
            self.load_rules(rules_path)

    def load_rules(self, path: str):
        """Load rules from a YAML file.

        Expected YAML structure::

            rules:
              - name: rule_name
                description: "..."
                match_type: expression | function | pattern | sql
                match_pattern: "regex or exact string"
                replacement: "PySpark template"
                priority: 80
                enabled: true
        """
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not raw or "rules" not in raw:
            return

        for entry in raw["rules"]:
            rule = CustomRule(
                name=entry.get("name", "unnamed"),
                description=entry.get("description", ""),
                match_type=entry.get("match_type", ""),
                match_pattern=entry.get("match_pattern", ""),
                replacement=entry.get("replacement", ""),
                priority=int(entry.get("priority", 50)),
                enabled=entry.get("enabled", True),
            )
            if rule.enabled:
                self.rules.append(rule)

        self.rules.sort(key=lambda r: -r.priority)

    def add_rule(self, rule: CustomRule):
        """Programmatically add a rule and re-sort."""
        self.rules.append(rule)
        self.rules.sort(key=lambda r: -r.priority)

    def apply_rules(
        self, informatica_expression: str, context: dict = None
    ) -> tuple[str, list[str]]:
        """Apply custom rules to an expression.

        Returns:
            (converted_text, list_of_applied_rule_names)

        Replacement templates support:
            {match}             - the full matched text
            {group1}..{groupN}  - regex capture groups
            {table}, {column}, {schema} - values from *context* dict
        """
        context = context or {}
        text = informatica_expression
        applied: list[str] = []

        for rule in self.rules:
            try:
                text, was_applied = self._apply_single(rule, text, context)
                if was_applied:
                    applied.append(rule.name)
            except Exception:
                # A broken rule should never crash the pipeline.
                continue

        return text, applied

    def validate_rules(self) -> list[str]:
        """Validate all loaded rules and return a list of issues (empty = OK)."""
        issues: list[str] = []
        seen_names: set[str] = set()

        for rule in self.rules:
            if not rule.name:
                issues.append("Rule with empty name found.")
            if rule.name in seen_names:
                issues.append(f"Duplicate rule name: '{rule.name}'.")
            seen_names.add(rule.name)

            if not rule.match_pattern:
                issues.append(f"Rule '{rule.name}': empty match_pattern.")

            # Validate regex compiles
            try:
                re.compile(rule.match_pattern)
            except re.error as exc:
                issues.append(f"Rule '{rule.name}': invalid regex — {exc}")

            if not rule.replacement:
                issues.append(f"Rule '{rule.name}': empty replacement.")

            if rule.match_type not in ("expression", "function", "pattern", "sql", ""):
                issues.append(
                    f"Rule '{rule.name}': unknown match_type '{rule.match_type}'."
                )

            if not 0 <= rule.priority <= 100:
                issues.append(
                    f"Rule '{rule.name}': priority {rule.priority} outside 0-100 range."
                )

        return issues

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_single(
        rule: CustomRule, text: str, context: dict
    ) -> tuple[str, bool]:
        """Apply a single rule. Returns (new_text, was_applied)."""
        pattern = re.compile(rule.match_pattern)
        match = pattern.search(text)
        if not match:
            return text, False

        def _replacer(m: re.Match) -> str:
            replacement = rule.replacement

            # {match} -> full matched text
            replacement = replacement.replace("{match}", m.group(0))

            # {group1}, {group2}, ... -> capture groups
            for i, grp in enumerate(m.groups(), start=1):
                if grp is not None:
                    replacement = replacement.replace(f"{{group{i}}}", grp)

            # Context variables: {table}, {column}, {schema}, etc.
            for key, val in context.items():
                replacement = replacement.replace(f"{{{key}}}", str(val))

            return replacement

        new_text = pattern.sub(_replacer, text)
        return new_text, new_text != text
