"""Every skill and command must expose parseable YAML frontmatter.

Claude Code reads the frontmatter to learn a skill's `description` (its
discovery trigger) and a command's `allowed-tools`/`argument-hint`.  When the
YAML fails to parse it does not error -- it loads the file with *empty
metadata*, silently dropping every field.  A skill in that state is invisible:
nothing ever triggers it.

Two ways a plain (unquoted) scalar breaks, both of which shipped:

    description: ... (verbs: inventory, plan)   # ": " starts a nested mapping
    argument-hint: [--region us-east-1]         # "[" starts a flow sequence

This is a lint rather than a YAML parse so the suite stays dependency-free
(README and TESTING both promise boto3 is the only requirement).  It was
cross-checked against PyYAML while it was written: it flags exactly the files
PyYAML rejects and none of the ones it accepts.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Characters that give a *plain* scalar special meaning at its first position.
_LEADING_INDICATORS = "[]{}>|*&!%#@`,?:-"
_FRONTMATTER = re.compile(r"\A---\n(.*?)\n---\n", re.S)


def frontmatter_files() -> list[Path]:
    paths = sorted(REPO_ROOT.glob("skills/**/SKILL.md"))
    paths += sorted(REPO_ROOT.glob("commands/*.md"))
    return [p for p in paths if _FRONTMATTER.match(p.read_text(encoding="utf-8"))]


def scalar_problem(value: str) -> str | None:
    """Why a plain scalar would not survive a YAML parse."""
    text = value.strip()
    if not text:
        return None
    if text[0] in "\"'":
        return None                      # quoted: any content is safe
    if text[0] in _LEADING_INDICATORS:
        return f"starts with the YAML indicator {text[0]!r}; quote the value"
    if ": " in text or text.endswith(":"):
        return "contains ': ', which YAML reads as a nested mapping; quote the value"
    if " #" in text:
        return "contains ' #', which YAML reads as a comment; quote the value"
    return None


class PluginFrontmatterTests(unittest.TestCase):
    def test_files_with_frontmatter_are_present(self):
        # Guards the glob itself: a rename must not silently empty this suite.
        names = {p.name for p in frontmatter_files()}
        self.assertIn("SKILL.md", names)
        # Slash commands are a Claude Code concept. The Codex tree ships the same
        # skills and tests but drives the verbs over MCP, so it has no commands/
        # directory -- asserting them unconditionally fails there, the same way
        # test_translator_followups failed on a live_teardown.py that is not
        # shipped. Require them only where the directory exists.
        if (REPO_ROOT / "commands").is_dir():
            self.assertTrue(
                {"inventory.md", "plan.md", "migrate.md", "verify.md"} <= names
            )

    def test_every_frontmatter_value_survives_a_yaml_parse(self):
        for path in frontmatter_files():
            block = _FRONTMATTER.match(path.read_text(encoding="utf-8")).group(1)
            for line in block.splitlines():
                if not line.strip() or line.startswith((" ", "\t", "#")):
                    continue
                key, _, value = line.partition(":")
                if not _:
                    continue
                problem = scalar_problem(value)
                rel = path.relative_to(REPO_ROOT)
                with self.subTest(file=str(rel), key=key.strip()):
                    self.assertIsNone(
                        problem,
                        f"{rel} frontmatter key {key.strip()!r} {problem}. "
                        "Claude Code drops ALL metadata for a file whose "
                        "frontmatter fails to parse.",
                    )

    def test_skill_declares_a_name_and_description(self):
        skill = REPO_ROOT / "skills" / "aws-aidp-migrator" / "SKILL.md"
        block = _FRONTMATTER.match(skill.read_text(encoding="utf-8")).group(1)
        keys = {line.partition(":")[0].strip()
                for line in block.splitlines() if ":" in line}
        self.assertIn("name", keys)
        self.assertIn("description", keys)


if __name__ == "__main__":
    unittest.main(verbosity=2)
