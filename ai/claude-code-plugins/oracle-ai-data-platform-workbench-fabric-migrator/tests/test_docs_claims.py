"""Every claim this repository makes about itself, checked against the code.

A false claim here is worse than a missing one: it is something a reader will
act on. So each assertion below re-takes the measurement rather than trusting
the prose, and the prose states numbers that were actually measured.
"""
import ast
import hashlib
import json
import os
import re
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]


def _flat(text: str) -> str:
    """Lowercase with runs of whitespace collapsed.

    A phrase check must survive line wrapping: prose gets re-wrapped every time
    it is edited, and "**not\nexecution-verified**" is the same claim as the
    unwrapped form. Matching on raw text would make this test fail for a reason
    that has nothing to do with the claim.
    """
    return re.sub(r"\s+", " ", text.lower()).replace("*", "")


class ReadmeTests(unittest.TestCase):
    def setUp(self):
        self.text = (ROOT / "README.md").read_text(encoding="utf-8")
        self.lower = _flat(self.text)

    def test_states_pass_is_not_execution_verified(self):
        self.assertIn("not execution-verified", self.lower)

    def test_states_that_writing_to_aidp_is_opt_in(self):
        """This used to assert the README said "does not write to AIDP". It
        stopped being true when `publish` landed, and the test caught that --
        which is the job. The claim it guards now is the accurate one: the
        default is offline and writing takes an explicit flag."""
        self.assertIn("nothing is written to aidp unless you ask", self.lower)
        self.assertIn("dry run by default", self.lower)
        self.assertIn("--apply", self.lower)

    def test_states_publish_never_overwrites(self):
        self.assertIn("never overwrites", self.lower)

    def test_every_publish_apply_example_passes_a_prefix(self):
        """`--apply` refuses to run without `--prefix`, so an example
        without one is a command that fails when copied. The README's did."""
        for doc in ("README.md", "docs/RUNBOOK.md"):
            text = (ROOT / doc).read_text(encoding="utf-8")
            # A command continued with `\` is one example over several lines.
            commands = re.sub(r"\\\n\s*", " ", text).splitlines()
            examples = [c for c in commands
                        if "fabric-aidp publish" in c and "--apply" in c]
            self.assertTrue(examples, doc)
            for line in examples:
                with self.subTest(doc=doc, line=line):
                    self.assertIn("--prefix", line)

    def test_names_the_fixture_limitation(self):
        self.assertIn("fixture", self.lower)

    def test_does_not_overclaim(self):
        for phrase in ("fully automated", "guaranteed", "production-ready",
                       "no manual work", "one click"):
            with self.subTest(phrase=phrase):
                self.assertNotIn(phrase, self.lower)

    def test_shows_the_four_verbs(self):
        for verb in ("inventory", "plan", "migrate", "verify"):
            with self.subTest(verb=verb):
                self.assertIn(f"fabric-aidp {verb}", self.text)

    def test_documents_the_demo_with_no_tenant(self):
        # One demo, one input, and it is staged rather than packaged.
        self.assertIn("make demo", self.lower)
        self.assertIn("110 assets", self.lower)
        self.assertNotIn("--fixture demo", self.text)

    def test_has_a_status_section_naming_what_is_not_done(self):
        self.assertIn("not implemented", self.lower)

    def test_does_not_claim_to_flag_all_tsql_control_flow(self):
        """T2. The README said "Stored procedures and any T-SQL control
        flow". Measured on the tree that said it: IF, BEGIN...END, RETURN,
        THROW, RAISERROR, WAITFOR and BEGIN TRANSACTION/COMMIT all came back
        unchanged with findings=[], and Spark 4.2.0 rejects every one. What
        the tool detects is a list, and the README has to be that list."""
        for phrase in ("any t-sql control flow",
                       "anything with t-sql control flow"):
            with self.subTest(phrase=phrase):
                self.assertNotIn(phrase, self.lower)

    def test_names_the_control_flow_it_actually_detects(self):
        for construct in ("declare @", "set @", "begin…end", "while", "goto",
                          "return", "throw", "raiserror", "waitfor",
                          "commit", "rollback", "cursors", "exec"):
            with self.subTest(construct=construct):
                self.assertIn(construct, self.lower)

    def test_says_a_temp_table_is_named_rather_than_refused_whole(self):
        """T8. `#temp` used to trip the procedural gate, which is why it sat
        in the control-flow sentence. It is SQ51_TEMP_TABLE now and the rest
        of the object is translated, so the README has to say which."""
        self.assertIn("#temp", self.lower)
        self.assertIn("rather than refused whole", self.lower)

    def test_says_the_list_is_the_whole_of_what_is_detected(self):
        self.assertIn("control flow outside it is not detected", self.lower)

    def test_does_not_claim_the_fixture_fires_every_rule(self):
        """It said "including that every flag actually fires". It fires 47 of
        194; see FixtureCoverageTests for the measurement. The phrase is
        banned by name because it is the specific sentence that was false, and
        the ones near it read as harmless."""
        for phrase in ("every flag actually fires",
                       "every flag fires",
                       "covers every feature"):
            with self.subTest(phrase=phrase):
                # assertFalse, not assertNotIn: a failed assertNotIn prints
                # the whole README, and the useful half of that message is
                # the phrase.
                self.assertFalse(phrase in self.lower,
                                 f"README still claims {phrase!r}")

    def test_tells_a_windows_reader_the_make_targets_will_not_work(self):
        """`PY := .venv/bin/python` and a bash `demo.sh`. The tool is portable
        and the wrapper is not, and only one of those was written down."""
        self.assertTrue("posix shell" in self.lower,
                        "README does not warn that `make` is POSIX-only")
        self.assertTrue(r"\scripts\python" in self.lower,
                        "README gives no Windows command to run instead")


class SkillClaimTests(unittest.TestCase):
    """The skill file is what an agent reads instead of the README, so a
    claim corrected in one and not the other is still being made."""

    @classmethod
    def setUpClass(cls):
        cls.lower = _flat((ROOT / "skills" / "fabric-aidp-migrator"
                           / "SKILL.md").read_text(encoding="utf-8"))

    def test_does_not_claim_to_flag_all_tsql_control_flow(self):
        for phrase in ("any t-sql control flow",
                       "anything with t-sql control flow"):
            with self.subTest(phrase=phrase):
                self.assertNotIn(phrase, self.lower)

    def test_tells_the_agent_to_say_the_list_is_finite(self):
        self.assertIn("control flow outside the list reads as clean",
                      self.lower)

class SupportingDocTests(unittest.TestCase):
    def test_changelog_exists_and_names_the_version(self):
        text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        self.assertIn("0.1.0", text)

    def test_notice_and_privacy_exist(self):
        self.assertTrue((ROOT / "NOTICE").is_file())
        self.assertTrue((ROOT / "PRIVACY.md").is_file())

    def test_privacy_states_no_data_leaves_the_machine(self):
        text = _flat((ROOT / "PRIVACY.md").read_text(encoding="utf-8"))
        self.assertIn("no network", text)

    def test_demo_script_is_executable_and_uses_the_staged_input(self):
        path = ROOT / "demo.sh"
        self.assertTrue(path.is_file())
        if os.name != "nt":  # no exec bit on Windows; git still records 100755
            self.assertTrue(path.stat().st_mode & 0o111, "demo.sh must be executable")
        body = path.read_text(encoding="utf-8")
        # One demo, one input. There used to be a second target running the
        # real corpora separately, and nobody could tell which number counted.
        self.assertIn("demo-input", body)
        self.assertNotIn("--fixture demo", body)


class RunbookTests(unittest.TestCase):
    """The runbook is the document someone follows with a real workspace in
    front of them. A stale command there costs more than a stale sentence."""

    @classmethod
    def setUpClass(cls):
        cls.text = (ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
        cls.lower = _flat(cls.text)

    def test_it_exists_and_is_linked_from_the_readme(self):
        self.assertTrue(self.text.strip())
        readme = _flat((ROOT / "README.md").read_text(encoding="utf-8"))
        self.assertIn("docs/runbook.md", readme)

    def test_it_covers_every_verb(self):
        for verb in ("inventory", "plan", "migrate", "verify", "publish"):
            with self.subTest(verb=verb):
                self.assertIn(f"fabric-aidp {verb}", self.lower)

    def test_it_says_publish_is_the_only_step_that_writes(self):
        self.assertIn("only `publish` writes anywhere".lower(), self.lower)

    def test_it_tells_the_reader_to_dry_run_publish_first(self):
        self.assertIn("always dry-run it first", self.lower)

    def test_it_does_not_promise_execution(self):
        self.assertIn("pass does not mean it runs", self.lower)

    def test_it_carries_no_credentials(self):
        """No real OCID, and none of the fragments a live run once left here.
        The fragments are checked by digest -- see `_BANNED_DIGESTS`."""
        self.assertIsNone(_REAL_OCID.search(self.text), "a real OCID")
        self.assertEqual(_banned_tokens(self.text), set())

    def test_it_does_not_tell_anyone_to_pass_demo(self):
        """--demo stopped being required; a runbook still teaching it would
        spread a flag that does nothing."""
        self.assertNotIn("migrate out/plan.json -o out/migrated --demo", self.lower)

    def test_it_tells_the_two_unreadable_counts_apart(self):
        """`unreadable_items` is item-DIRECTORY scoped and reads like "files
        that could not be read". A reader who takes an empty list for "every
        file read" is wrong in the direction that matters: three undecodable
        `.sql` files leave it empty while `unreadable_object_count` is 3.
        Renaming the key would drop assets out of a plan built from an
        inventory.json already on disk, so the runbook carries the
        distinction instead."""
        for phrase in ("unreadable_object_count", "unreadable_items",
                       "item directories only",
                       "does not mean every file read"):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.lower)


class DemoScriptTests(unittest.TestCase):
    """demo.sh is the first thing anyone runs, so what it lists is what the
    tool appears to do. It filtered to .py/.sql/.md, which silently hid every
    AIDP workflow job -- the artifact that makes `publish` meaningful."""

    @classmethod
    def setUpClass(cls):
        cls.text = (ROOT / "demo.sh").read_text(encoding="utf-8")

    def test_it_lists_generated_job_files(self):
        self.assertIn("*.job.json", self.text)

    def test_it_lists_every_artifact_kind_migrate_can_write(self):
        for pattern in ("*.py", "*.sql", "*.md", "*.job.json", "*.html"):
            with self.subTest(pattern=pattern):
                self.assertIn(pattern, self.text)

    def test_it_still_says_pass_is_not_execution_verified(self):
        self.assertIn("not execution-verified", self.text)


class NamingDocTests(unittest.TestCase):
    """README and SKILL.md gave two different table-naming conventions, and
    neither matched the code."""

    @classmethod
    def setUpClass(cls):
        cls.readme = _flat((ROOT / "README.md").read_text(encoding="utf-8"))
        skill = ROOT / "skills" / "fabric-aidp-migrator" / "SKILL.md"
        cls.skill = _flat(skill.read_text(encoding="utf-8")) if skill.is_file() else ""

    def test_the_readme_states_the_rule(self):
        self.assertIn("default.saleslake.claim", self.readme)
        self.assertIn("default.acmedw.claim", self.readme)

    def test_the_readme_separates_catalog_from_namespace(self):
        self.assertIn("--catalog", self.readme)
        self.assertIn("not** `--namespace`".lower().replace("**", ""),
                      self.readme.replace("**", ""))

    def test_the_skill_does_not_give_a_second_convention(self):
        if not self.skill:
            self.skipTest("no SKILL.md")
        self.assertNotIn("default.dbo.", self.skill)

    def test_every_name_the_readme_documents_is_the_name_the_code_builds(self):
        """The README's naming table is three worked examples. Asserting the
        strings appear in the prose only proves the prose is unchanged; these
        run the examples through `naming.aidp_table`, which is the authority
        the spec names, so the table cannot quietly stop being true."""
        from fabric_aidp.naming import aidp_table
        rows = [
            # (item, table, schema)          -> documented AIDP name
            (("SalesLake", "claim", ""), "default.SalesLake.claim"),
            (("AcmeDW", "claim", "dbo"), "default.AcmeDW.claim"),
            (("AcmeDW", "t", "postgres_air"), "default.AcmeDW_postgres_air.t"),
            # The backticking example from the paragraph below the table.
            (("Sales Lake", "claim", ""), "default.`Sales Lake`.claim"),
        ]
        raw = (ROOT / "README.md").read_text(encoding="utf-8")
        for (item, table, schema), documented in rows:
            with self.subTest(documented=documented):
                built = aidp_table(item, table, schema=schema)
                self.assertEqual(built, documented)
                self.assertTrue(documented in raw,
                                f"{documented} is what the code builds; the "
                                f"README no longer shows it")



# ---------------------------------------------------------------------------
# D1: what the demo estate proves, and what it does not.


def _rule_universe():
    """Every rule id the translators define, and how that is counted.

    Two sources, because the code has two:

    1. String literals matching a rule-id shape anywhere in `fabric_aidp/`.
       That covers the ids written out in full -- the `Finding("NB01_...")`
       calls and the lookup tables whose values are rule ids.
    2. The families built at emit time from a lookup table, where the literal
       in the source is only a prefix: `f"SQ30_{name.upper()}"` and friends.
       Counting only (1) would understate the total by 35 and so overstate
       how much of the rule set anything exercises.

    Reading the private tables in (2) is deliberate. If one is renamed this
    raises, loudly, in a test whose whole job is to notice that the rule
    inventory changed -- which is the correct outcome, not a nuisance.
    """
    from fabric_aidp.translate import fabric_notebook_to_spark as nb
    from fabric_aidp.translate import tsql_to_spark_sql as tsql

    # A rule id is PREFIX + two digits + at least one _WORD. The trailing
    # `[A-Z0-9]+` (rather than `[A-Z0-9_]+`) is what keeps the f-string prefix
    # "NB21_MAGIC_" out: it is not an id, it is half of one.
    shape = re.compile(r"^[A-Z]{1,4}[0-9]{2}_[A-Z0-9]+(?:_[A-Z0-9]+)*$")
    ids = set()
    for path in sorted((ROOT / "fabric_aidp").rglob("*.py")):
        if "fixtures" in path.parts:
            continue  # notebook fixtures are Fabric sources, not Python
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if shape.match(node.value):
                    ids.add(node.value)

    for name in tsql._RENAME_FLAGS:
        ids.add("SQ30_%s" % name.upper())
    for name, spec in tsql._RENAMES.items():
        ids.add("SQ30_%s" % name.upper())
        if spec[1] is not None:          # arity is checked, so it can fail
            ids.add("SQ30_%s_ARITY" % name.upper())
    for key in tsql._TYPE_FLAGS:
        ids.add("SQ60_%s" % key.upper())
    for prefix in ("SQ81_CONVERT", "SQ84_TRY_CONVERT"):
        for suffix in ("_TYPE", "_TYPE_LOSS", "_STYLE", "_ARITY"):
            ids.add(prefix + suffix)     # _convert_builder, one path, two names
    for name in nb._FLAGGED_MAGICS:
        ids.add("NB21_MAGIC_%s" % name.upper())
    return ids


def _fired_by(export_dir):
    """(rule ids fired, asset count, verify summary) for a whole migration.

    The verify summary is here because the README prints it and nothing
    re-took it: the line read `PASS: 24  REVIEW: 79` for as long as anyone
    can see in the history while the demo had been ending `PASS: 23
    REVIEW: 80`, because every other number in this file is pinned and that
    one was prose. Running all four verbs is what the reader does, so this
    runs all four.
    """
    from fabric_aidp.inventory.manifest import ALL_SOURCES, build_manifest
    from fabric_aidp.migrate.runner import migrate
    from fabric_aidp.plan.planner import build_plan
    from fabric_aidp.verify.checker import verify

    manifest = build_manifest(Path(export_dir), ALL_SOURCES)
    plan = build_plan(manifest, oci_namespace="acmens")
    with TemporaryDirectory() as out:
        # `demo=` is gone -- #31 removed the flag and its parameter.
        report = migrate(plan, out_dir=Path(out))
        summary = verify(Path(out) / "report.json")["summary"]
    return ({f["rule"] for row in report["results"]
             for f in (row.get("findings") or [])},
            len(report["results"]), summary)


def _parser_available():
    from fabric_aidp.translate import m_parser
    return m_parser.parser_available()


@unittest.skipUnless(_parser_available(), "Node + mparse not installed")
class FixtureCoverageTests(unittest.TestCase):
    """The README claimed the fixture proved "every flag actually fires".

    It did not, and it cannot: a fixture authored alongside the rules reaches
    the rules someone thought to write an input for. The numbers below are the
    ones the README now prints, re-taken here on every run.

    Node-gated because the M translator is: without the Power Query parser the
    Dataflow rules never fire and the counts are lower for a reason that has
    nothing to do with the claim. The suite still passes with no Node -- this
    class skips.
    """

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, str(ROOT / "scripts"))
        import stage_demo_input

        from fabric_aidp.fixtures import demo_workspace_path

        cls.universe = _rule_universe()
        cls._tmp = TemporaryDirectory()
        staged = Path(cls._tmp.name) / "demo-input"
        stage_demo_input.build(staged)
        cls.demo_fired, cls.demo_assets, cls.demo_verdicts = _fired_by(staged)
        cls.acme_fired, _, _ = _fired_by(demo_workspace_path())
        cls.readme = (ROOT / "README.md").read_text(encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_demo_estate_does_not_fire_every_rule(self):
        """The claim itself, independent of any number. This is the assertion
        that must never be satisfiable again by editing prose: whatever the
        counts become, a fixture that reaches every rule is the thing the
        README used to say and the code has never done."""
        self.assertLess(len(self.demo_fired), len(self.universe))
        self.assertLess(len(self.acme_fired), len(self.demo_fired))

    def test_every_rule_the_demo_fires_is_one_the_translators_define(self):
        """Guards the measurement rather than the claim. If a fired id is not
        in the universe, `_rule_universe` has a hole and every ratio above it
        is wrong -- which would make the README's figure flattering by
        accident."""
        self.assertEqual(sorted(self.demo_fired - self.universe), [])

    def test_the_readme_prints_the_counts_that_were_measured(self):
        """Exact, not a ratchet, and the choice is deliberate.

        A ratchet ("at least 47 of at least 194") would stay green while the
        real figure drifted to 47 of 300 -- a README stating a measurement
        nobody took, guarded by a passing test. That is the exact failure this
        file exists to prevent, so the two numbers are pinned.

        The cost is real and was paid during the batch that wrote this: an
        unrelated branch added one rule id and one fixture firing, and both
        numbers moved. That edit is one line of prose, and having to make it
        is the point -- a coverage claim whose denominator moved is stale even
        though its shape did not.
        """
        match = re.search(
            r"fire \*\*(\d+) of the (\d+) distinct rule ids\*\*", self.readme)
        self.assertIsNotNone(
            match, "README no longer states the measured rule coverage")
        stated_fired, stated_total = int(match.group(1)), int(match.group(2))
        self.assertEqual(
            (stated_fired, stated_total),
            (len(self.demo_fired), len(self.universe)),
            "README says the demo estate fires %d of %d rule ids; it fires "
            "%d of %d. Update the sentence in 'Limits worth knowing'."
            % (stated_fired, stated_total,
               len(self.demo_fired), len(self.universe)))

    def test_the_readme_prints_the_synthetic_estates_own_count(self):
        """The Acme estate is the part 'authored alongside the rules', and it
        is the weaker of the two numbers. Stating only the larger one would
        flatter the fixture the sentence is actually about."""
        match = re.search(r"Acme estate on its own fires \*\*(\d+)\*\*",
                          self.readme)
        self.assertIsNotNone(match, "README no longer states the Acme count")
        self.assertEqual(int(match.group(1)), len(self.acme_fired),
                         "README says %s; it is %d"
                         % (match.group(1), len(self.acme_fired)))

    def test_the_readme_and_the_skill_agree_on_the_coverage_figure(self):
        """Two documents, one measurement. They disagreed about table naming
        once already."""
        skill = _flat((ROOT / "skills" / "fabric-aidp-migrator" / "SKILL.md"
                       ).read_text(encoding="utf-8"))
        self.assertIn("%d of the %d rule ids"
                      % (len(self.demo_fired), len(self.universe)), skill)

    def test_the_demo_still_migrates_the_asset_count_the_readme_claims(self):
        self.assertEqual(self.demo_assets, 110)
        self.assertIn("110 assets", self.readme)

    def test_the_readme_prints_the_verdict_line_the_demo_ends_on(self):
        """The one number in this section that was prose and not a pin.

        It said `PASS: 24  REVIEW: 79  SKIP: 3  FAIL: 0` while the demo
        ended `PASS: 23  REVIEW: 80  SKIP: 3  FAIL: 0` -- a reader running
        `make demo` got a different answer from the one the README told
        them to expect, and nothing anywhere noticed.
        """
        match = re.search(
            r"Ends `PASS: (\d+)\s+REVIEW: (\d+)\s+SKIP: (\d+)\s+FAIL: (\d+)`",
            self.readme)
        self.assertIsNotNone(
            match, "README no longer states the demo's verdict line")
        stated = dict(zip(("PASS", "REVIEW", "SKIP", "FAIL"),
                          (int(g) for g in match.groups())))
        self.assertEqual(stated, dict(self.demo_verdicts))


# ---------------------------------------------------------------------------
# D2 / D3: what the plugin surface says the CLI does.


def _cli_options(verb):
    """Every option string the real parser accepts for one verb."""
    from fabric_aidp.cli import build_parser

    for action in build_parser()._subparsers._group_actions[0].choices[verb]._actions:
        for option in action.option_strings:
            yield option


def _documented_invocations(text):
    """(verb, [tokens]) for every `fabric-aidp <verb> ...` line in a document.

    Line continuations are joined first: the runbook wraps long publish
    commands, and half a command parses as a different command.
    """
    joined = re.sub(r"\\\n\s*", " ", text)
    for line in joined.splitlines():
        line = line.strip().lstrip("$ ").strip()
        if not line.startswith("fabric-aidp "):
            continue
        tokens = line.split("#", 1)[0].split()
        if len(tokens) >= 2 and not tokens[1].startswith("-"):
            yield tokens[1], tokens[2:]


PLUGIN_DOCS = sorted((ROOT / "commands").glob("*.md")) + [
    ROOT / "skills" / "fabric-aidp-migrator" / "SKILL.md",
    ROOT / "README.md",
    ROOT / "docs" / "RUNBOOK.md",
]


class PluginSurfaceTests(unittest.TestCase):
    """`commands/` and `skills/` are what a plugin user sees. They are also
    the only documentation an agent reads before running the tool, so a false
    line here is executed rather than merely believed."""

    @classmethod
    def setUpClass(cls):
        cls.files = {p: p.read_text(encoding="utf-8") for p in PLUGIN_DOCS
                     if p.is_file()}
        cls.commands = {p.stem: t for p, t in cls.files.items()
                        if p.parent.name == "commands"}

    def test_every_verb_the_cli_has_is_documented_as_a_command(self):
        """`publish` existed in the CLI, the README and the runbook, and
        nowhere in the plugin. A plugin user could not discover the one verb
        that writes to their workspace."""
        from fabric_aidp.cli import build_parser
        verbs = set(build_parser()._subparsers._group_actions[0].choices)
        self.assertEqual(verbs - set(self.commands), set())

    def test_the_plugin_surface_tells_the_user_dataflows_need_node(self):
        """Nothing under skills/ or commands/ said `npm`, `node_modules` or
        `mparse`. Without that step every Dataflow is counted and none is
        translated -- reported, since this week, but still not migrated, and
        the plugin never mentioned the one command that fixes it."""
        surface = "\n".join(
            text for path, text in self.files.items()
            if path.parent.name == "commands"
            or path.parent.name == "fabric-aidp-migrator").lower()
        for token in ("npm install", "mparse", "node"):
            with self.subTest(token=token):
                self.assertTrue(token in surface,
                                f"nothing under commands/ or skills/ says "
                                f"{token!r}")

    def test_no_command_file_claims_the_tool_never_writes_to_aidp(self):
        """`publish --apply` writes. "never writes to AIDP" was reassuring and
        false; "dry run by default, never overwrites" is reassuring and true."""
        for name, text in self.commands.items():
            with self.subTest(command=name):
                flat = _flat(text)
                self.assertNotIn("never writes to aidp", flat)
                self.assertNotIn("this tool never writes", flat)

    def test_migrate_does_not_teach_the_dead_demo_flag(self):
        flat = _flat(self.commands["migrate"])
        self.assertNotIn("--demo is required", flat)
        self.assertIn("accepted and ignored", flat)

    def test_migrate_asks_for_the_blocked_count_too(self):
        """A blocked object is work the user still owes. Reporting ok /
        needs-review / planned and stopping makes a migration with 42 refusals
        read as finished."""
        self.assertIn("blocked", _flat(self.commands["migrate"]))

    def test_verify_documents_that_a_filter_cannot_hide_a_failure(self):
        flat = _flat(self.commands["verify"])
        self.assertIn("never hidden by a filter", flat)
        self.assertIn("exit", flat)

    def test_publish_is_described_as_dry_run_first(self):
        flat = _flat(self.commands["publish"])
        self.assertIn("--apply", flat)
        self.assertIn("never overwrites", flat)
        self.assertIn("dry run", flat)

    def test_the_skill_does_not_say_inventory_runs_the_whole_pipeline(self):
        """`fabric-aidp inventory --fixture demo` writes a manifest and stops.
        Told it ran the pipeline, an agent reports numbers that never match
        the README's, because they are a manifest's numbers."""
        skill = _flat(self.files[
            ROOT / "skills" / "fabric-aidp-migrator" / "SKILL.md"])
        self.assertNotIn("runs the whole pipeline", skill)
        self.assertIn("writes a manifest and stops", skill)

    def test_the_skill_names_all_five_verbs(self):
        skill = self.files[ROOT / "skills" / "fabric-aidp-migrator" / "SKILL.md"]
        for verb in ("inventory", "plan", "migrate", "verify", "publish"):
            with self.subTest(verb=verb):
                self.assertIn(verb, skill)
        self.assertNotIn("in four\nverbs", skill)
        self.assertIn("five verbs", _flat(skill))

    def test_the_skill_gives_an_install_path(self):
        skill = self.files[ROOT / "skills" / "fabric-aidp-migrator" / "SKILL.md"]
        self.assertIn("CLAUDE_PLUGIN_ROOT", skill)
        self.assertIn("pyproject.toml", skill)


class DocumentedCliTests(unittest.TestCase):
    """Every flag the documentation types must be a flag the parser has.

    Cheap, and it is the only check that spans all six documents. A renamed
    option is otherwise found by the first person who copies a command out of
    the runbook.
    """

    @classmethod
    def setUpClass(cls):
        from fabric_aidp.cli import build_parser
        cls.verbs = set(build_parser()._subparsers._group_actions[0].choices)

    def test_every_documented_flag_exists_on_that_verb(self):
        for path in PLUGIN_DOCS:
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8")
            for verb, tokens in _documented_invocations(text):
                with self.subTest(doc=path.name, verb=verb):
                    self.assertIn(verb, self.verbs)
                    valid = set(_cli_options(verb))
                    used = {t.split("=", 1)[0] for t in tokens
                            if t.startswith("-")}
                    self.assertEqual(
                        used - valid, set(),
                        f"{path.name} shows `fabric-aidp {verb}` with flags "
                        f"the parser does not have")

    def test_every_slice_name_the_docs_use_is_a_real_source(self):
        """`--filter dataflow` was advertised by argparse and refused by the
        runner for a release. The six names now come from one table."""
        from fabric_aidp.sources import ALL_SOURCES
        pattern = re.compile(r"--(?:filter|sources)\s+([a-z,]+)")
        for path in PLUGIN_DOCS:
            if not path.is_file():
                continue
            for raw in pattern.findall(path.read_text(encoding="utf-8")):
                for slice_name in raw.split(","):
                    with self.subTest(doc=path.name, slice=slice_name):
                        self.assertIn(slice_name, ALL_SOURCES)


# ---------------------------------------------------------------------------
# Privacy, changelog, and what is in the tree.


class PrivacyClaimTests(unittest.TestCase):
    """PRIVACY.md said the tool makes no network calls, reads only the export
    directory, and requires no credentials. `publish --apply` shells out to
    the `aidp` CLI; every verb loads a `.env`; and publish reads three AIDP
    keys out of the environment. Three claims, three contradictions."""

    @classmethod
    def setUpClass(cls):
        cls.text = (ROOT / "PRIVACY.md").read_text(encoding="utf-8")
        cls.flat = _flat(cls.text)

    def test_it_does_not_claim_the_whole_tool_is_offline(self):
        """The old first sentence, "This tool makes no network calls", with
        nothing qualifying it."""
        self.assertNotIn("this tool makes no network calls", self.flat)
        self.assertNotIn("this tool makes no network", self.flat)

    def test_it_names_the_verbs_that_are_offline_and_the_one_that_is_not(self):
        self.assertIn("make no network calls", self.flat)
        self.assertIn("publish --apply", self.flat)
        for verb in ("inventory", "plan", "migrate", "verify"):
            with self.subTest(verb=verb):
                self.assertIn(verb, self.flat)

    def test_it_says_a_dotenv_is_read_by_every_verb(self):
        """`load_dotenv()` moved into `main()` this week, so it is no longer
        an `inventory`-only behaviour -- and it was never documented here."""
        self.assertIn(".env", self.text)
        self.assertIn("every verb", self.flat)

    def test_it_names_the_key_prefixes_the_dotenv_loader_accepts(self):
        """The prefix filter is a security property, not a detail: an
        unfiltered .env could set PATH or NODE_OPTIONS."""
        from fabric_aidp._env import KEY_PREFIXES
        for prefix in KEY_PREFIXES:
            with self.subTest(prefix=prefix):
                self.assertIn(prefix, self.text)

    def test_it_does_not_claim_no_credentials_are_read(self):
        self.assertNotIn("none are required or read", self.flat)
        for name in ("AIDP_WORKSPACE_KEY", "AIDP_CLUSTER_KEY",
                     "AIDP_INSTANCE_ID"):
            with self.subTest(name=name):
                self.assertIn(name, self.text)

    def test_it_names_the_marker_file_migrate_writes(self):
        from fabric_aidp.migrate.runner import IN_PROGRESS_MARKER
        self.assertIn(IN_PROGRESS_MARKER, self.text)

    def test_a_dotenv_cannot_be_committed_by_accident(self):
        """PRIVACY.md and the runbook both tell you to put AIDP keys in a
        `.env`. It was not in .gitignore."""
        ignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        self.assertIn(".env", [line.strip() for line in ignore])


class ChangelogTests(unittest.TestCase):
    """The changelog described a four-verb tool that translated notebooks and
    T-SQL. By then it also translated Power Query and pipelines and had a
    fifth verb that writes to a workspace."""

    @classmethod
    def setUpClass(cls):
        cls.flat = _flat((ROOT / "CHANGELOG.md").read_text(encoding="utf-8"))

    def test_it_names_every_verb(self):
        for verb in ("inventory", "plan", "migrate", "verify", "publish"):
            with self.subTest(verb=verb):
                self.assertIn(verb, self.flat)

    def test_it_names_every_translator_the_readme_advertises(self):
        for feature in ("power query", "pipeline", "shortcut", "t-sql",
                        "notebook"):
            with self.subTest(feature=feature):
                self.assertIn(feature, self.flat)

    def test_it_says_publish_is_the_only_thing_that_writes(self):
        self.assertIn("only verb that writes", self.flat)

    def test_it_states_the_prefix_requirement_as_breaking_with_the_fix(self):
        # `publish --apply` without `--prefix` used to publish into the
        # workspace root; it now exits 2. A script written against a
        # development checkout breaks on that, so the changelog has to say
        # so and say what to change -- not only that a prefix is used.
        self.assertIn("breaking", self.flat)
        self.assertIn("--prefix is required with --apply", self.flat)
        self.assertIn("add `--prefix <name>`", self.flat)


class PackageMetadataTests(unittest.TestCase):
    """What a package index shows. The description named three of six
    sources, and there was no readme, no project URL and no classifier."""

    @classmethod
    def setUpClass(cls):
        cls.text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    def test_the_description_names_the_sources_the_readme_advertises(self):
        line = next(line for line in self.text.splitlines()
                    if line.startswith("description ="))
        for source in ("Dataflow", "Pipeline", "Warehouse", "notebook"):
            with self.subTest(source=source):
                self.assertIn(source, line)

    def test_the_readme_is_the_long_description(self):
        self.assertIn('readme = "README.md"', self.text)

    def test_notice_ships_with_the_licence(self):
        """NOTICE records that the third-party corpora are redistributed
        under their own licences. It was in neither the wheel nor the sdist."""
        self.assertIn("NOTICE", self.text)

    def test_the_python_floor_matches_what_the_readme_promises(self):
        self.assertIn('requires-python = ">=3.9"', self.text)
        self.assertIn("3.9", (ROOT / "README.md").read_text(encoding="utf-8"))


# SHA-256 (first 16 hex) of each banned identifier, lower-cased. To check a
# word by hand: `python -c "import hashlib,sys;print(hashlib.sha256(sys.argv[1]
# .lower().encode()).hexdigest()[:16])" <word>` and look for it below. Plain
# text would publish the identifiers in every sdist, which is the leak this
# guards against. Categories, in order: other tenants' AIDP catalogs (5), a
# personal account and repository (2), a live job key (1), live instance-OCID
# fragments (3), tenancy names (3), an internal npm host (1).
_BANNED_DIGESTS = frozenset({
    "78851a4d31ed1088", "898b2afaf6fc8c5b", "ffed8924f243a425",
    "bcf3e6f8eacdb99b", "c2d7fcdfbeb1e2b8",
    "6c86216be028e55b", "61d4ead2fba534d4",
    "6dab45f431cd4a8d",
    "5bd12d9855eda032", "ae44dc5eabc37923", "e4d86e533b8207de",
    "79357b10f58544df", "fd556520dbee95d5", "ab9c1d5e6538abb0",
    "a8fb6c8d9526d3d8",
})
# A real OCID: the RUNBOOK's placeholder `ocid1.aidataplatform.oc1.<region>`
# does not match, a pasted one does.
_REAL_OCID = re.compile(r"ocid1\.[a-z0-9]+\.oc1\.[a-z0-9-]*\.[a-z0-9]{20,}")


def _banned_tokens(text):
    """Banned digests found among `text`'s words, split both with and
    without hyphens so a job key and an OCID fragment are both whole words."""
    lowered = text.lower()
    words = set(re.findall(r"[a-z0-9_]+", lowered)) | set(re.findall(r"[a-z0-9_-]+", lowered))
    return {w for w in words
            if hashlib.sha256(w.encode()).hexdigest()[:16] in _BANNED_DIGESTS}


class TreeHygieneTests(unittest.TestCase):
    """Identifiers that belong to a person or another tenant, in a repository
    whose own manifests say it publishes to oracle-samples."""

    @classmethod
    def setUpClass(cls):
        cls.files = {}
        for path in ROOT.rglob("*"):
            if not path.is_file():
                continue
            parts = set(path.parts)
            if parts & {".git", ".venv", "node_modules", "build", "dist",
                        "demo-input", "demo-output", ".superpowers",
                        "__pycache__"}:
                continue
            if path.suffix in (".pyc", ".png", ".gz", ".whl"):
                continue
            try:
                cls.files[path] = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue

    def test_no_banned_identifier_anywhere(self):
        """Other tenants' catalog names, a personal account and repository,
        a live job key, tenancy names and an internal host -- every one of
        which reached this tree or its history once. Checked by digest so
        that this file, which ships in the sdist, no longer carries them:
        it used to exempt itself because it named every one in plain text,
        which put them in every published copy. This file is scanned too."""
        hits = {str(p.relative_to(ROOT)): sorted(found)
                for p, text in self.files.items()
                for found in [_banned_tokens(text)] if found}
        self.assertEqual(hits, {})

    def test_no_real_ocid_anywhere(self):
        hits = [str(p.relative_to(ROOT)) for p, text in self.files.items()
                if _REAL_OCID.search(text)]
        self.assertEqual(hits, [])

    def test_no_home_directory_paths(self):
        hits = [str(p.relative_to(ROOT)) for p, text in self.files.items()
                if "/Users/" in text and p.suffix in (".md", ".toml", ".cfg")]
        self.assertEqual(hits, [])


class MakefileClaimTests(unittest.TestCase):
    """`make check` ends by telling a reviewer how to read the two numbers.
    It explained the skip count with a cause that is not the cause."""

    @classmethod
    def setUpClass(cls):
        cls.flat = _flat((ROOT / "Makefile").read_text(encoding="utf-8"))

    def test_it_does_not_blame_an_absent_customer_dataflow_corpus(self):
        """Measured: with Node installed, 15 of the 16 skips need pyspark and
        a JVM and 1 needs the fuller Warehouse corpus. None needs a dataflow
        corpus, and none of the corpora here is customer data -- they are
        public-repo samples, and 14 of them are vendored."""
        self.assertNotIn("customer dataflows", self.flat)
        self.assertNotIn("deliberately not in this repo", self.flat)

    def test_it_names_the_reasons_tests_actually_skip(self):
        self.assertIn("pyspark", self.flat)
        self.assertIn("jvm", self.flat)


@unittest.skipUnless(_parser_available(), "Node + mparse not installed")
class RunbookOutputTests(unittest.TestCase):
    """The runbook opens with "Every command here has been run. Where a step
    shows expected output, that output is real, not illustrative."

    That is a claim about the document, so it gets checked like one. The
    blocks in steps 2 to 5 are the bundled demo estate, so every number in
    them is reproducible here. Two of them had drifted: the migrate sample
    showed `changes=6 flags=1` for a Dataflow that now reports
    `changes=5 flags=3`, and warehouse asset ids had gained the item name.

    Node-gated: the Dataflow row only exists when the parser does.
    """

    @classmethod
    def setUpClass(cls):
        from fabric_aidp.fixtures import demo_workspace_path
        from fabric_aidp.inventory.manifest import ALL_SOURCES, build_manifest
        from fabric_aidp.migrate.runner import migrate
        from fabric_aidp.plan.planner import build_plan
        from fabric_aidp.verify.checker import verify

        cls.text = (ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
        manifest = build_manifest(demo_workspace_path(), ALL_SOURCES)
        cls.plan = build_plan(manifest, oci_namespace="acmens")
        cls._tmp = TemporaryDirectory()
        out = Path(cls._tmp.name)
        cls.report = migrate(cls.plan, out_dir=out)
        cls.verdicts = verify(out / "report.json")["summary"]

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_plan_total_it_prints_is_the_plan_total(self):
        self.assertIn("total assets: %d" % len(self.plan["assets"]), self.text)

    def test_the_migrate_counts_it_prints_are_the_migrate_counts(self):
        counts = self.report["counts"]
        line = ("# done. ok=%d  needs_review=%d  blocked=%d  planned=%d  "
                "error=%d" % (counts.get("ok", 0),
                              counts.get("needs_manual_review", 0),
                              counts.get("blocked", 0),
                              counts.get("planned", 0),
                              counts.get("error", 0)))
        self.assertIn(line, self.text)

    def test_the_verify_block_it_prints_is_the_verify_result(self):
        for verdict in ("PASS", "REVIEW", "SKIP", "FAIL"):
            with self.subTest(verdict=verdict):
                self.assertRegex(
                    self.text,
                    r"%s:\s+%d\b" % (verdict, self.verdicts[verdict]))

    def test_every_asset_id_it_shows_is_an_asset_id_the_run_produces(self):
        """The sample rows named `warehouse.view.dbo.v_open_claims`. The
        runner emits `warehouse.AcmeDW.view.dbo.v_open_claims`; the item is in
        the id because two warehouses can both define `dbo.CurrentDate`. A
        reader grepping the report for the documented id finds nothing."""
        produced = {row.get("asset_id") for row in self.report["results"]}
        shown = re.findall(r"^  (?:OK|REVIEW)\s+(\S+)\s+changes=",
                           self.text, re.MULTILINE)
        self.assertTrue(shown, "the runbook no longer shows any sample rows")
        for asset_id in shown:
            with self.subTest(asset_id=asset_id):
                self.assertIn(asset_id, produced)

    def test_the_sample_rows_show_the_counts_the_run_reports(self):
        by_id = {row.get("asset_id"): row for row in self.report["results"]}
        rows = re.findall(r"^  (?:OK|REVIEW)\s+(\S+)\s+changes=(\d+) flags=(\d+)",
                          self.text, re.MULTILINE)
        self.assertTrue(rows)
        for asset_id, changes, flags in rows:
            with self.subTest(asset_id=asset_id):
                self.assertIn(asset_id, by_id,
                              "the runbook shows a row for an asset this "
                              "migration does not produce")
                row = by_id[asset_id]
                self.assertEqual((int(changes), int(flags)),
                                 (row.get("changes", 0), row.get("flags", 0)))

    def test_the_inventory_block_shows_the_fields_the_summary_prints(self):
        """`coverage_unknown_count` and `metadata_unreadable` were added to
        the summary and never to the sample, so a reader comparing the two
        would think their own run had printed something extra."""
        from fabric_aidp.fixtures import demo_workspace_path
        from fabric_aidp.inventory.manifest import (ALL_SOURCES, build_manifest,
                                                    summarize)
        summary = summarize(build_manifest(demo_workspace_path(), ALL_SOURCES))
        for line in summary.splitlines():
            line = line.strip()
            if not line.startswith(("notebook ", "warehouse ", "lakehouse ",
                                    "pipeline ", "semanticmodel ", "dataflow ")):
                continue
            for field in re.findall(r"(\w+)=", line):
                with self.subTest(field=field):
                    self.assertIn(field, self.text)


class SkipReasonTests(unittest.TestCase):
    """The README tells a reviewer that `OK (skipped=N)` is a pass and names
    the reasons. That is only useful if it is the whole list.

    Read statically rather than by running the suite: a docs test that runs
    the suite to count its own skips is both slow and circular.
    """

    # The count word the README's sentence may use -> the number it means.
    COUNT_WORDS = {"two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
                   "seven": 7, "eight": 8}

    # The phrase the README uses for each cause -> the substrings a skip
    # reason may use to mean it.
    DOCUMENTED = {
        "and a jvm are not installed": ("pyspark",),
        "node is not installed": ("node", "mparse"),
        "a fuller corpus is not present": ("corpus", "fixtures not present"),
        "a distribution cannot be built here": ("pep 517", "wheel", "sdist"),
    }

    # Reasons that describe the *repository*, not the machine. A reviewer
    # cannot hit these by lacking a tool, so they are not something the
    # README's list is about; they fire only if someone changed the tree.
    REPOSITORY_CONDITIONS = frozenset({
        "no SKILL.md",
        "the bundled dataflow did not parse",
        "the sdist no longer ships tests at all, which is "
        "also a consistent answer",
    })

    @classmethod
    def setUpClass(cls):
        cls.reasons = set()
        for path in sorted((ROOT / "tests").glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "attr", None) or getattr(
                    node.func, "id", None)
                if name in ("skipUnless", "skipIf"):
                    arg = node.args[1] if len(node.args) > 1 else None
                elif name in ("skip", "skipTest", "SkipTest"):
                    # `raise unittest.SkipTest(...)` counts: a helper that
                    # skips on a missing tool is the same promise to a
                    # reviewer as a decorator that does, and leaving it out
                    # was a hole in this check.
                    arg = node.args[0] if node.args else None
                else:
                    continue
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    cls.reasons.add((path.name, arg.value))
                elif isinstance(arg, ast.JoinedStr):
                    cls.reasons.add((path.name, "".join(
                        v.value for v in arg.values
                        if isinstance(v, ast.Constant))))

    def test_the_suite_has_skips_to_explain(self):
        self.assertTrue(self.reasons)

    def test_every_skip_reason_is_one_the_readme_documents(self):
        allowed = {sub for subs in self.DOCUMENTED.values() for sub in subs}
        for filename, reason in sorted(self.reasons):
            if reason in self.REPOSITORY_CONDITIONS:
                continue
            with self.subTest(file=filename, reason=reason):
                self.assertTrue(
                    any(sub in reason.lower() for sub in allowed),
                    f"{filename} skips for a reason the README does not list: "
                    f"{reason!r}. Add it to 'Testing' or reword the skip.")

    def test_the_readme_lists_each_cause(self):
        readme = _flat((ROOT / "README.md").read_text(encoding="utf-8"))
        for cause in self.DOCUMENTED:
            with self.subTest(cause=cause):
                self.assertTrue(cause in readme,
                                f"README's Testing section no longer names "
                                f"the cause {cause!r}")

    @staticmethod
    def _readme_skip_list():
        """(the number the count word claims, the bullets that follow it).

        The list is the run of `- ` bullets immediately after the sentence,
        which is how the section is written and how a reader reads it.
        """
        text = (ROOT / "README.md").read_text(encoding="utf-8")
        match = re.search(
            r"there are exactly \*{0,2}(\w+)\*{0,2}\s+reasons a test here skips",
            re.sub(r"\s+", " ", text), flags=re.IGNORECASE)
        assert match, "README no longer states how many skip reasons there are"
        bullets, started = 0, False
        for line in text.splitlines():
            if not started:
                started = "reasons a test here skips" in line or (
                    "reasons a test here" in line)
                continue
            if line.startswith("- "):
                bullets += 1
            elif line.startswith("#") or line.startswith("`tests/"):
                break
        return match.group(1).lower(), bullets

    def test_the_readme_counts_its_own_list(self):
        """The paragraph said "exactly three reasons" over four bullets, and
        this class did not notice: it checked the reasons against the suite
        and never checked the count word against the length of its own list.
        The sentence claims a total, so the total is what must be checked."""
        word, bullets = self._readme_skip_list()
        self.assertIn(word, self.COUNT_WORDS,
                      f"README counts its skip reasons with {word!r}, which is "
                      f"not a number word this test knows")
        self.assertEqual(
            self.COUNT_WORDS[word], bullets,
            f"README says there are exactly {word} reasons a test skips and "
            f"then lists {bullets} of them")

    def test_the_readme_count_is_the_number_of_causes_this_file_checks(self):
        """The two halves of the claim, tied together. `DOCUMENTED` is the
        list `test_the_readme_lists_each_cause` walks; if the sentence and
        that map disagree, one of them is describing a different README."""
        word, _bullets = self._readme_skip_list()
        self.assertEqual(self.COUNT_WORDS[word], len(self.DOCUMENTED),
                         f"README says exactly {word} skip reasons; this file "
                         f"checks {len(self.DOCUMENTED)}")


class ParseGateScopeTests(unittest.TestCase):
    """Which generated files "generated PySpark is `ast.parse`d" is about.

    `m_to_pyspark` runs `ast.parse` over the whole Dataflow script it builds
    and raises rather than write one that does not compile. Nothing does the
    same for notebooks -- and nothing should: a translated notebook is
    *Fabric notebook source*, which carries `%%sql` cell magics, and those
    cells are left byte-for-byte alone on purpose, because in a Fabric
    notebook `%%sql` is Spark SQL and applying the T-SQL rules to it would
    corrupt valid code.

    The README sentence was unqualified, and a reader who took it as covering
    every generated `.py` was being told the opposite of what ships. Both
    halves are checked here: the artifact really does not parse, and the
    prose really does say which translator it means.
    """

    @classmethod
    def setUpClass(cls):
        from fabric_aidp.fixtures import demo_workspace_path
        from fabric_aidp.inventory import build_manifest
        from fabric_aidp.migrate import migrate
        from fabric_aidp.plan.planner import build_plan
        cls._tmp = TemporaryDirectory()
        out = Path(cls._tmp.name)
        migrate(build_plan(build_manifest(demo_workspace_path())), out_dir=out)
        cls.emitted = {p.relative_to(out).as_posix():
                       p.read_text(encoding="utf-8")
                       for p in sorted(out.rglob("*.py"))}

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _unparseable(self):
        failures = {}
        for name, text in self.emitted.items():
            try:
                ast.parse(text)
            except SyntaxError as exc:
                failures[name] = exc
        return failures

    def test_a_notebook_the_demo_emits_is_not_parseable_python(self):
        """Measured 2026-09-29: `notebooks/06_Sql_Summary.py`, one of the
        seven .py files the bundled Acme estate produces, status `ok`, bare
        `%%sql` on line 15. If this ever stops being true, the README
        paragraph it justifies has to be re-read, not deleted."""
        failures = self._unparseable()
        self.assertTrue(
            failures,
            "every emitted .py now parses -- re-read the README's 'Only "
            "Dataflow output is parse-checked' paragraph before relaxing this")
        for name, exc in failures.items():
            with self.subTest(artifact=name):
                self.assertTrue(
                    name.startswith("notebooks/"),
                    f"{name} does not parse and is not a notebook, so the "
                    f"README's scoping of the gate is now wrong: {exc}")
                self.assertIn("%%",
                              self.emitted[name].splitlines()[exc.lineno - 1])

    def test_the_readme_says_which_translator_the_parse_gate_belongs_to(self):
        readme = _flat((ROOT / "README.md").read_text(encoding="utf-8"))
        self.assertIn("the pyspark a dataflow generates is `ast.parse`d", readme)
        self.assertIn("only dataflow output is parse-checked", readme)
        self.assertIn("no other translator has an equivalent whole-file gate",
                      readme)


class RuleNumberTests(unittest.TestCase):
    """Two people spent a number on two different rules, and nothing noticed.

    `main` gained `PL17_NOTEBOOK_NOT_IN_THIS_RUN`; a branch cut earlier
    landed `PL17_NOTEBOOK_PARAMETER`. Git merged them clean -- different
    files -- and the collision was semantic: one asset row carrying `PL17`
    twice, at two severities, reading as one rule in the report. The branch
    renumbered by hand, after review. Nothing in the suite would have said
    so.

    Not asserted here, because both are false of this tree:

      * "one number, one id". 30 numbers deliberately carry several. `SQ30`
        has 19 -- one per renamed T-SQL function -- `NB21` 7, one per
        flagged magic, and `PL13_RETRY` / `PL13_TIMEOUT` are the same shape
        as the PL17 pair and are both correct. A number here is a rule
        *family*, and the families are the point.
      * "one id, one severity". Five ids vary it on purpose, and
        `NB25_LAKEHOUSE_TWO_BUCKETS` explains in its own docstring why the
        severity cannot be decided from one literal.

    What is asserted is the set of families itself, exactly, in the spirit
    FixtureCoverageTests states above: a new id on a number that already
    carries an *unrelated* one makes that number newly shared and turns this
    red, while a 20th `SQ30_` -- a real member of a real family -- does not,
    because `SQ30` is already listed. A reviewer then either renumbers or
    adds the number here, which is a deliberate act with a name on it.
    """

    # MEASURED on this tree. 30 numbers, each a family whose members are
    # facets of one rule. Add to this list only when the new id genuinely
    # belongs to the family already there; if it does not, it wants a free
    # number, and `RuleNumberTests` exists because nothing else will say so.
    FAMILIES = frozenset({
        "NB07",   # a cell that cannot be read: untokenizable, or no source
        "NB20",   # the Tables/ path and the table name it resolves to
        "NB21",   # one per flagged Fabric magic
        "PL13",   # activity policy: retry and timeout
        "SQ01", "SQ02", "SQ10", "SQ11", "SQ14", "SQ15", "SQ19", "SQ20",
        "SQ21", "SQ22",
        "SQ30",   # one per renamed T-SQL function, plus its arity check
        "SQ31", "SQ32", "SQ41", "SQ50", "SQ51",
        # SQ52: T-SQL's optional INTO, written in. Two statements have one --
        # INSERT and MERGE -- and they are one concern: Spark requires the
        # keyword, T-SQL does not, and without it the target is the one name
        # `_OBJECT_KEYWORDS` cannot see. The MERGE half arrived with this
        # batch, and this test is what made it declare itself.
        "SQ52",
        "SQ60",   # one per unsupported T-SQL type
        "SQ61", "SQ70", "SQ76",
        # SQ77: Delta column mapping, the rewrite and the refusal.
        # Arrived with #11 after this list was written, and this test
        # is what made it declare itself rather than slip in.
        "SQ77", "SQ80", "SQ81", "SQ82", "SQ83", "SQ84",
        "SQ90",
    })

    @staticmethod
    def _by_number(ids):
        grouped = {}
        for rule in ids:
            number = re.match(r"^([A-Z]{1,4}[0-9]{2})_", rule).group(1)
            grouped.setdefault(number, set()).add(rule)
        return grouped

    def setUp(self):
        self.grouped = self._by_number(_rule_universe())

    def test_every_rule_id_has_the_shape_a_number_can_be_read_out_of(self):
        self.assertEqual(len(self.grouped) > 0, True)
        for rule in _rule_universe():
            self.assertRegex(rule, r"^[A-Z]{1,4}[0-9]{2}_[A-Z0-9_]+$")

    def test_exactly_the_declared_numbers_carry_more_than_one_rule(self):
        shared = {n for n, members in self.grouped.items() if len(members) > 1}
        unexpected = sorted(shared - self.FAMILIES)
        self.assertEqual(
            unexpected, [],
            "these numbers now name more than one rule, and were not declared "
            "families: %s. Either the new id belongs to the family already "
            "there -- add the number to FAMILIES and say so -- or it wants a "
            "free number. Members: %s"
            % (unexpected, {n: sorted(self.grouped[n]) for n in unexpected}))

    def test_no_declared_family_has_quietly_become_a_single_rule(self):
        """The other direction. A family that lost its siblings is a stale
        entry in FAMILIES, and a stale entry is a hole this test no longer
        guards."""
        stale = sorted(n for n in self.FAMILIES if len(self.grouped.get(n, ())) < 2)
        self.assertEqual(stale, [], f"no longer families: {stale}")

    def test_it_would_have_caught_the_collision_it_was_written_for(self):
        """The PL17 case, replayed. Not a mock of the checker: the real
        grouping, over the real universe plus the one id that collided."""
        collided = set(_rule_universe()) | {"PL17_NOTEBOOK_PARAMETER"}
        self.assertIn("PL17_NOTEBOOK_NOT_IN_THIS_RUN", collided,
                      "main's PL17 is gone; this test no longer replays anything")
        grouped = self._by_number(collided)
        shared = {n for n, members in grouped.items() if len(members) > 1}
        self.assertIn("PL17", shared - self.FAMILIES)

    def test_the_ids_this_pr_added_are_on_free_numbers(self):
        for rule in ("PL21_NOTEBOOK_PARAMETER", "PL22_PARAMETER_DEFAULT",
                     "PL23_PARAMETER_EXPRESSION", "PL24_PARAMETER_RETYPED",
                     "NB35_PARAMETER_NOT_RE_READ",
                     "NB36_PARAMETER_CELL_UNPARSEABLE",
                     "NB37_TASK_PARAMETER_IGNORED"):
            number = rule.split("_", 1)[0]
            with self.subTest(rule=rule):
                self.assertEqual(self.grouped.get(number), {rule})



class SparkRuntimeClaimTests(unittest.TestCase):
    """The README records which Spark the behavioural claims were measured on.

    Most comments in this repo name pyspark 4.2.0, which is what the local
    probe has. The AIDP cluster runs 3.5.0 with `ansi.enabled` FALSE, where
    4.2.0 defaults it TRUE -- so a claim taken on the probe is not
    automatically a claim about the target. The four places the two
    genuinely differ are listed in the README and corrected at each site;
    this pins the list so it cannot quietly become five.
    """

    @classmethod
    def setUpClass(cls):
        cls.readme = (ROOT / "README.md").read_text(encoding="utf-8")
        cls.lower = _flat(cls.readme)

    def test_it_names_the_version_the_cluster_runs(self):
        """The TABLE ROW, not the prose. `assertIn("3.5.0", readme)` passed
        with the row corrupted, because the version also appears four times
        in the paragraphs under it -- an assertion that cannot tell the
        measurement from the discussion of it."""
        rows = {line.strip() for line in self.readme.splitlines()
                if line.strip().startswith("|")}
        self.assertIn("| `spark.version` | **3.5.0** |", rows)
        self.assertIn("| `spark.sql.ansi.enabled` | **false** |", rows)
        self.assertIn("| `spark.sql.sources.default` | **delta** |", rows)

    def test_it_says_the_local_probe_is_a_different_version(self):
        """Without this the table reads as if every number came from the
        target, which is the mistake that made the sweep necessary."""
        self.assertIn("pyspark 4.2.0", self.readme)
        self.assertIn("defaults", self.lower)

    def test_it_names_each_divergence(self):
        for phrase in ("collate utf8_lcase", "withcolumnsrenamed",
                       "do not parse", "hyphenated table name"):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.lower)

    def test_the_divergences_are_recorded_where_the_decision_is_made(self):
        """A list in the README is a reference; the reason has to be beside
        the code, or the next reader re-derives it."""
        sites = {
            "fabric_aidp/translate/tsql_to_spark_sql.py": "3.5.0",
            "fabric_aidp/translate/m_runtime.py": "3.5.0",
            "fabric_aidp/translate/m_to_pyspark.py": "3.5.0",
        }
        for path, needle in sites.items():
            with self.subTest(path=path):
                self.assertIn(needle, (ROOT / path).read_text(encoding="utf-8"))

    def test_no_source_file_claims_a_bare_spark_measurement(self):
        """"Measured on Spark" with no version is the shape that started
        this: a reader cannot tell whether it applies to the target."""
        offenders = []
        for path in sorted((ROOT / "fabric_aidp").rglob("*.py")):
            if "node_modules" in path.parts or "fixtures" in path.parts:
                continue
            text = path.read_text(encoding="utf-8")
            # The version may wrap onto the next comment or docstring line,
            # which a naive lookahead reads as absent -- it flagged a claim
            # that does name 4.2.0, one line down. So the lookahead skips
            # one line break plus its leading `#` or indentation.
            pattern = re.compile(
                r"[Mm]easured on (?:real )?Spark\b"
                r"(?![ \t]*(?:\n[ \t]*#?[ \t]*)?\d)")
            for match in pattern.finditer(text):
                line = text[:match.start()].count("\n") + 1
                offenders.append("%s:%d" % (path.name, line))
        self.assertEqual(offenders, [], "\n".join(offenders))


class ExecutionVerifiedClaimTests(unittest.TestCase):
    """The README says what has been run on AIDP, and what has not.

    "PASS is not execution-verified" is the load-bearing honesty in this
    tool's reporting. A section saying some of it now IS verified has to be
    exact about which part, or it quietly promotes the whole estate. The
    part that was run is the publish path, the job structure, the SQL cell
    magic and the table naming. The translated bodies -- 27 notebooks, 22
    warehouse artifacts, 4 Dataflow scripts -- were not.
    """

    @classmethod
    def setUpClass(cls):
        cls.readme = (ROOT / "README.md").read_text(encoding="utf-8")
        cls.lower = _flat(cls.readme)

    def test_pass_is_still_not_execution_verified(self):
        """The claim the whole report rests on. If this ever goes, it should
        go deliberately and not as a side effect of adding a success story."""
        self.assertIn("not**\nexecution-verified", self.readme.replace("\r", ""))

    def test_it_names_what_was_not_run(self):
        for phrase in ("were published but not executed",
                       "no onelake path was exercised"):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.lower)

    def test_it_says_the_evidence_was_the_row_count_not_the_exit_status(self):
        """A job that reports SUCCESS having run nothing is the exact failure
        this tool exists to catch; a run reported as evidence has to say
        which it was."""
        self.assertIn("returned **2 groups**", self.readme)
        self.assertIn("not the exit status", self.lower)

    def test_it_says_the_upstream_failure_was_expected(self):
        self.assertIn("expected one rather than a finding", self.lower)

    def test_it_says_the_objects_were_deleted(self):
        self.assertIn("deleted afterwards", self.lower)

if __name__ == "__main__":
    unittest.main()
