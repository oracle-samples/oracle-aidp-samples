import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMMANDS = ("inventory", "plan", "migrate", "verify", "publish")
PLUGIN_NAME = "oracle-ai-data-platform-workbench-fabric-migrator"


def _frontmatter(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}
    _, block, _rest = text.split("---\n", 2)
    fields = {}
    for line in block.splitlines():
        if ":" in line and not line.startswith(" "):
            key, _, value = line.partition(":")
            fields[key.strip()] = value.strip().strip('"')
    return fields


class ManifestTests(unittest.TestCase):
    def test_plugin_json_is_valid_and_named(self):
        data = json.loads((ROOT / ".claude-plugin" / "plugin.json").read_text())
        self.assertEqual(data["name"], PLUGIN_NAME)
        for key in ("version", "description", "author", "license", "commands"):
            self.assertIn(key, data)

    def test_marketplace_lists_this_plugin(self):
        data = json.loads((ROOT / ".claude-plugin" / "marketplace.json").read_text())
        self.assertEqual([p["name"] for p in data["plugins"]], [PLUGIN_NAME])

    def test_codex_manifest_is_valid(self):
        data = json.loads((ROOT / ".codex-plugin" / "plugin.json").read_text())
        self.assertEqual(data["name"], PLUGIN_NAME)

    def test_the_claude_manifest_points_at_the_commands_directory(self):
        """Kept as the contrast to the Codex one below: `commands` is a
        Claude Code concept and belongs in exactly one of the two files."""
        data = json.loads((ROOT / ".claude-plugin" / "plugin.json").read_text())
        self.assertEqual(data["commands"], "./commands/")
        self.assertEqual(data["mcpServers"], "./.mcp.json")

    def test_mcp_config_runs_the_server_from_the_plugin_root(self):
        """A plugin installed from a marketplace has the package on disk and
        no console script on PATH. MEASURED with `claude mcp list` after a
        marketplace install: `fabric-aidp-mcp` -> "Failed to connect --
        ENOENT: Executable not found in $PATH". Running the module with the
        plugin root on PYTHONPATH needs nothing installed but the `mcp` SDK,
        and is the launcher the published AWS migrator plugin uses."""
        server = json.loads((ROOT / ".mcp.json").read_text())["mcpServers"]["fabric-aidp-migrator"]
        self.assertEqual(server["command"], "python3")
        self.assertEqual(server["args"], ["-m", "fabric_aidp.mcp_server"])
        self.assertEqual(server["env"]["PYTHONPATH"], "${CLAUDE_PLUGIN_ROOT}")

    def test_the_install_for_that_script_reaches_a_reader_before_a_client_does(self):
        """`.mcp.json` names a console script whose SDK is an optional extra,
        so a plain `pip install -e .` leaves an MCP client pointed at a
        command that exits immediately.

        The file stays: `.claude-plugin/plugin.json` points at it,
        `MANIFEST.in` ships it, and it is the only thing in the tree that
        advertises the server at all. What was missing is that the install
        instruction lived in one line of the Compatibility section, past
        everything someone installing the tool reads, so the first place most
        people met the requirement was a failed server in a client log. It is
        now in Quick start and in the failure itself, and this ties the three
        spellings of the extra together so they cannot drift.
        """
        extra = "pip install -e '.[mcp]'"
        pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertRegex(pyproject, r"(?m)^mcp = \[",
                         "the `[mcp]` extra the install line names")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn(extra, readme)
        self.assertIn(".mcp.json", readme)
        # And for the reader who never opens the README, because their client
        # opened the config file for them.
        source = (ROOT / "fabric_aidp" / "mcp_server.py").read_text(encoding="utf-8")
        self.assertIn(extra, source)
        self.assertIn(".mcp.json", source)


class CommandTests(unittest.TestCase):
    def test_every_verb_has_a_command_file(self):
        for verb in COMMANDS:
            with self.subTest(verb=verb):
                self.assertTrue((ROOT / "commands" / f"{verb}.md").is_file())

    def test_every_command_has_a_description(self):
        for verb in COMMANDS:
            with self.subTest(verb=verb):
                fields = _frontmatter(ROOT / "commands" / f"{verb}.md")
                self.assertTrue(fields.get("description"))

    def test_every_command_names_the_cli(self):
        for verb in COMMANDS:
            with self.subTest(verb=verb):
                body = (ROOT / "commands" / f"{verb}.md").read_text(encoding="utf-8")
                self.assertIn(f"fabric-aidp {verb}", body)


class SkillTests(unittest.TestCase):
    PATH = ROOT / "skills" / "fabric-aidp-migrator" / "SKILL.md"

    def test_skill_exists_with_name_and_description(self):
        fields = _frontmatter(self.PATH)
        self.assertEqual(fields.get("name"), "fabric-aidp-migrator")
        self.assertTrue(fields.get("description"))

    def test_skill_states_the_deterministic_contract(self):
        body = self.PATH.read_text(encoding="utf-8").lower()
        self.assertIn("flagged", body)
        self.assertIn("never", body)

    def test_skill_does_not_overclaim_pass(self):
        body = self.PATH.read_text(encoding="utf-8").lower()
        self.assertIn("not execution-verified", body)
        for phrase in ("ready to run", "guaranteed", "production-ready"):
            self.assertNotIn(phrase, body)



class CodexManifestTests(unittest.TestCase):
    """`.codex-plugin/plugin.json` was a byte-for-byte copy of the Claude one.

    The two formats do not coincide. Checked against the published schema at
    https://agent-plugins.org/schemas/1.0.0/plugin.schema.json (the one the
    Codex plugin docs tell you to declare), on 2026-09-29:

      * `required` is `["$schema", "name"]`, and the copy had no `$schema`;
      * `additionalProperties` is `false` over exactly the ten properties
        listed below, and the copy carried two more -- `commands`, which is a
        Claude Code concept with no Codex equivalent, and `mcpServers`, which
        in this format lives in a sibling `mcp.json`, not the manifest;
      * `name` is constrained to a lowercase pattern of at most 64 characters.

    So the old file was not "the same format by luck": it was a manifest that
    happened to parse as JSON. Skills need no field at all -- they are
    discovered by convention from a root `skills/` directory, which this
    repository has.

    The schema is transcribed rather than fetched: the suite runs with no
    network, and a test that silently skips when offline would not have
    caught this.
    """

    # https://agent-plugins.org/schemas/1.0.0/plugin.schema.json, 2026-09-29.
    ALLOWED = {"$schema", "name", "version", "description", "author",
               "homepage", "repository", "license", "keywords", "extensions"}
    REQUIRED = {"$schema", "name"}
    SCHEMA_URL = "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json"
    NAME_RE = re.compile(r"^(?!.*(?:--|\.\.))[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
    AUTHOR_KEYS = {"name", "email", "url"}

    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / ".codex-plugin" / "plugin.json"
        cls.data = json.loads(cls.path.read_text(encoding="utf-8"))

    def test_it_declares_the_schema_it_claims_to_follow(self):
        self.assertEqual(self.data.get("$schema"), self.SCHEMA_URL)

    def test_it_has_every_required_property(self):
        self.assertEqual(self.REQUIRED - set(self.data), set())

    def test_it_has_no_property_the_schema_forbids(self):
        """`additionalProperties: false`. A key this format does not define
        is a validation error, not a harmless extra."""
        self.assertEqual(set(self.data) - self.ALLOWED, set())

    def test_it_does_not_carry_claude_only_keys(self):
        for key in ("commands", "mcpServers", "hooks", "skills"):
            with self.subTest(key=key):
                self.assertNotIn(key, self.data)

    def test_the_name_matches_the_schemas_pattern(self):
        name = self.data["name"]
        self.assertLessEqual(len(name), 64)
        self.assertRegex(name, self.NAME_RE)
        self.assertEqual(name, PLUGIN_NAME)

    def test_the_author_object_has_only_the_keys_the_schema_defines(self):
        self.assertEqual(set(self.data.get("author", {})) - self.AUTHOR_KEYS,
                         set())

    def test_it_is_not_a_copy_of_the_claude_manifest(self):
        """The regression itself. Two client formats, one file, and the only
        reason nobody noticed is that both are JSON."""
        claude = (ROOT / ".claude-plugin" / "plugin.json").read_text(
            encoding="utf-8")
        self.assertNotEqual(self.path.read_text(encoding="utf-8"), claude)
        self.assertNotEqual(json.loads(claude), self.data)

    def test_the_skill_codex_discovers_by_convention_is_there(self):
        """No manifest field points at it: the format discovers skills from a
        root `skills/` directory. If that moved, the Codex plugin would ship
        with nothing in it and still validate."""
        self.assertTrue(
            (ROOT / "skills" / "fabric-aidp-migrator" / "SKILL.md").is_file())

    def test_both_manifests_describe_the_same_tool(self):
        claude = json.loads(
            (ROOT / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8"))
        self.assertEqual(claude["name"], self.data["name"])
        self.assertEqual(claude["version"], self.data["version"])
        self.assertEqual(claude["license"], self.data["license"])


if __name__ == "__main__":
    unittest.main()
