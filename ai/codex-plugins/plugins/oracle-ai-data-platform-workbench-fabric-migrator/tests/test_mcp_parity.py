import contextlib
import inspect
import io
import sys
import types
import unittest
from unittest import mock

from fabric_aidp.cli import ALL_SOURCES, build_parser

#: Values that have to be real because the CLI restricts them with `choices`.
_PLACEHOLDERS = {"fixture": "demo", "filter": "notebook",
                 "sources": ",".join(ALL_SOURCES)}

#: Number words for the count of registered tools, so the module docstring
#: cannot go on claiming a count the file no longer has.
_COUNT_WORDS = {3: "three", 4: "four", 5: "five", 6: "six", 7: "seven"}


class _Completed:
    returncode = 0
    stdout = ""
    stderr = ""


def _argv_each_tool_builds(module) -> dict:
    """Every command line the MCP tools can produce, keyed by a label.

    Each tool is called with every parameter populated, so every optional
    flag it knows how to append is appended. Nothing executes: `subprocess.run`
    is replaced and only its argument list is kept. `inventory` is called
    twice because `fixture` and `export_dir` are exclusive in the tool body,
    so one call can never cover both.
    """
    calls = {}
    for name in ("inventory", "plan", "migrate", "verify", "publish"):
        function = getattr(module, name)
        kwargs = {}
        for parameter, spec in inspect.signature(function).parameters.items():
            if isinstance(spec.default, bool):
                kwargs[parameter] = True
            else:
                kwargs[parameter] = _PLACEHOLDERS.get(parameter, f"{parameter}.v")
        variants = {name: kwargs}
        if name == "inventory":
            variants["inventory (export_dir, not fixture)"] = dict(kwargs,
                                                                   fixture=None)
        for label, arguments in variants.items():
            seen = []
            with mock.patch("subprocess.run", side_effect=lambda argv, **kw: (
                    seen.append(list(argv)) or _Completed())):
                function(**arguments)
            # Drop [sys.executable, "-m", "fabric_aidp.cli"].
            calls[label] = seen[0][3:]
    return calls


def _cli_verbs():
    verbs = set()
    for action in build_parser()._actions:
        if getattr(action, "choices", None):
            verbs |= set(action.choices)
    return verbs


class _FakeMCP:
    def __init__(self, name):
        self.name = name
        self.registered = []

    def tool(self, *a, **kw):
        def decorate(fn):
            self.registered.append(fn.__name__)
            return fn
        return decorate

    def run(self):
        raise AssertionError("run() must not be called at import time")


def _load_with_stub():
    """Import the server with the MCP SDK stubbed, so the test needs no extra dep."""
    instances = []

    def factory(name):
        instance = _FakeMCP(name)
        instances.append(instance)
        return instance

    fastmcp = types.ModuleType("mcp.server.fastmcp")
    fastmcp.FastMCP = factory
    server = types.ModuleType("mcp.server")
    server.fastmcp = fastmcp
    root = types.ModuleType("mcp")
    root.server = server
    sys.modules.pop("fabric_aidp.mcp_server", None)
    with mock.patch.dict(sys.modules, {"mcp": root, "mcp.server": server,
                                       "mcp.server.fastmcp": fastmcp}):
        import fabric_aidp.mcp_server as module
    sys.modules.pop("fabric_aidp.mcp_server", None)
    return module, instances[0]


class ParityTests(unittest.TestCase):
    def test_tool_names_match_the_cli_verbs_exactly(self):
        _module, server = _load_with_stub()
        self.assertEqual(set(server.registered), _cli_verbs())

    def test_every_verb_is_registered(self):
        _module, server = _load_with_stub()
        self.assertEqual(sorted(server.registered),
                         ["inventory", "migrate", "plan", "publish", "verify"])

    def test_publish_is_exposed_without_an_apply_switch(self):
        """Publishing writes to a live workspace. An agent may show what it
        would do; only a person at the CLI may do it."""
        module, server = _load_with_stub()
        import inspect
        signature = inspect.signature(module.publish)
        self.assertNotIn("apply", signature.parameters)

    def test_importing_does_not_start_the_server(self):
        module, _server = _load_with_stub()
        self.assertTrue(callable(module.main))

    def test_every_command_line_a_tool_builds_is_one_the_cli_parses(self):
        """Parity was checked at the level of verb names and nothing else.

        Measured 2026-09-29: renaming the CLI's `--tables-csv` to
        `--tables-file` left all six tests in this file green, while every
        `inventory(tables_csv=...)` call would have died on the CLI's
        "unrecognized arguments". Matching names is not parity -- the argv a
        tool builds has to be argv the CLI accepts. The flags happen to line
        up today (12 of 12); this is what keeps them there.
        """
        module, _server = _load_with_stub()
        parser = build_parser()
        for label, argv in sorted(_argv_each_tool_builds(module).items()):
            with self.subTest(tool=label):
                # argparse exits rather than raising on an unknown flag, and
                # writes the reason to stderr on the way out. A bare
                # `SystemExit: 2` would say nothing about which flag.
                stderr = io.StringIO()
                rejected = None
                with contextlib.redirect_stderr(stderr):
                    try:
                        parser.parse_args(argv)
                    except SystemExit:
                        rejected = stderr.getvalue().strip().splitlines()[-1]
                if rejected is not None:
                    self.fail("the CLI rejects the argv `%s` builds -- %s -- "
                              "%s" % (label, " ".join(argv), rejected))

    def test_the_module_docstring_counts_the_tools_the_file_registers(self):
        """It said "the four migration verbs" while registering five."""
        module, server = _load_with_stub()
        summary = module.__doc__.casefold()
        for count, word in _COUNT_WORDS.items():
            with self.subTest(word=word):
                if count == len(server.registered):
                    self.assertIn(word, summary)
                else:
                    self.assertNotIn(word, summary)

    def test_the_sources_a_tool_documents_are_the_sources_the_cli_has(self):
        """`inventory`'s `sources:` line listed five of the six; it predates
        `dataflow`. An agent reads the tool docstring and never sees the
        CLI's help, so a source missing from it is one the agent cannot know
        to ask for."""
        module, _server = _load_with_stub()
        for source in sorted(ALL_SOURCES):
            with self.subTest(source=source):
                self.assertIn(source, module.inventory.__doc__)

    def test_migrate_offers_an_agent_no_mode_to_choose(self):
        """The tool docstring once called `--demo` "offline mode ... Required
        today", which is advice to pass a flag that does nothing, given on the
        one surface an agent reads. Then it was "accepted and ignored" -- and
        the parameter still defaulted to True, so every MCP call appended a
        `--demo` to argv. The flag is gone from the CLI, so the parameter had
        to go with it or this tool would build argv the CLI rejects."""
        module, _server = _load_with_stub()
        self.assertNotIn("required", module.migrate.__doc__.casefold())
        self.assertNotIn("demo", inspect.signature(module.migrate).parameters)

    def test_every_tool_has_a_docstring(self):
        module, server = _load_with_stub()
        for name in server.registered:
            with self.subTest(tool=name):
                self.assertTrue((getattr(module, name).__doc__ or "").strip())

    def test_missing_sdk_raises_a_helpful_message(self):
        """This is what an MCP client configured from the repo's `.mcp.json`
        gets after a plain `pip install -e .`, which does not install the
        extra. The reader is looking at a server log with no surrounding
        context and did not type the command, so the message has to name the
        install, say why the dependency is optional rather than missing by
        mistake, name the file that sent the client here, and say that the
        CLI is unaffected. "pip install" alone left three of those unsaid.
        """
        sys.modules.pop("fabric_aidp.mcp_server", None)
        with mock.patch.dict(sys.modules, {"mcp": None, "mcp.server": None,
                                           "mcp.server.fastmcp": None}):
            with self.assertRaises(SystemExit) as ctx:
                import fabric_aidp.mcp_server  # noqa: F401
        message = str(ctx.exception)
        self.assertIn("pip install -e '.[mcp]'", message)
        self.assertIn("3.10", message)
        self.assertIn(".mcp.json", message)
        self.assertIn("fabric-aidp", message)
        sys.modules.pop("fabric_aidp.mcp_server", None)


if __name__ == "__main__":
    unittest.main()
