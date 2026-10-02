"""What a `pip install` actually gets.

Everything else in this suite runs against the source tree, where an editable
install makes the whole repository importable. That hides packaging bugs
completely: `make demo` worked while a real `pip install` produced a CLI whose
own documented first command failed with "no Fabric items", because
`demo-workspace/**/*` does not match a leading dot and every `.platform` was
dropped -- the file Fabric uses to identify an item at all.

The wheel build is slow enough to be worth doing once, so one class builds it
and reads the result.
"""
from __future__ import annotations

import ast
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import pathlib
import re
import warnings
import unittest
import zipfile
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _build_wheel(into: Path):
    try:
        done = subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "-q",
             "-w", str(into), str(ROOT)],
            capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        # A non-zero exit was already a skip; a stall was not. pip builds in
        # its own isolated environment, which means fetching a backend, which
        # means this call is a network call. MEASURED: TimeoutExpired is not a
        # SkipTest, so it propagated out of setUpClass and errored the class
        # -- the one shape the README's "degrades to a skip" does not cover.
        raise unittest.SkipTest(
            "pip wheel did not finish in 600s; it fetches its own build "
            "backend, so no wheel can be built here without network")
    if done.returncode != 0:
        raise unittest.SkipTest(
            f"cannot build a wheel here: {done.stderr.strip()[:200]}")
    wheels = list(into.glob("*.whl"))
    if not wheels:
        raise unittest.SkipTest("pip wheel produced nothing")
    return wheels[0]


class WheelContentsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        with zipfile.ZipFile(_build_wheel(Path(cls._tmp.name))) as archive:
            cls.names = archive.namelist()

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_demo_fixture_keeps_its_platform_files(self):
        """Fabric identifies an item *by* its .platform; without them the
        bundled estate is not a workspace, it is a pile of folders."""
        on_disk = len(list(
            (ROOT / "fabric_aidp" / "fixtures" / "demo-workspace").rglob(".platform")))
        in_wheel = sum(1 for n in self.names if n.endswith("/.platform"))
        self.assertEqual(in_wheel, on_disk)
        self.assertGreater(in_wheel, 0)

    def test_the_power_query_parser_ships(self):
        """At `parents[2]/mparse` this resolved to site-packages/mparse once
        installed, so an installed copy could never translate a Dataflow."""
        for name in ("fabric_aidp/mparse/parse.js",
                     "fabric_aidp/mparse/package.json",
                     "fabric_aidp/mparse/package-lock.json"):
            with self.subTest(name=name):
                self.assertIn(name, self.names)

    def test_node_modules_does_not_ship(self):
        """400 files the user installs with npm, not payload for a wheel."""
        self.assertEqual([n for n in self.names if "node_modules" in n], [])

    def test_the_vendored_corpora_do_not_ship(self):
        """Third-party fixtures are a test corpus, not part of the tool."""
        self.assertEqual([n for n in self.names if "fixtures/real" in n], [])


class InstalledLayoutTests(unittest.TestCase):
    """The parser is found relative to the package, so one code path works
    from a source tree and from site-packages."""

    def test_the_parser_directory_sits_inside_the_package(self):
        from fabric_aidp.translate import m_parser
        package_root = Path(m_parser.__file__).resolve().parents[1]
        self.assertEqual(m_parser.MPARSE_DIR.parent, package_root)

    def test_the_parse_script_is_where_the_module_looks_for_it(self):
        from fabric_aidp.translate import m_parser
        self.assertTrue(m_parser.PARSE_JS.is_file())

    def test_the_demo_fixture_is_reachable_from_the_package(self):
        from fabric_aidp.fixtures import demo_workspace_path
        items = list(Path(demo_workspace_path()).rglob(".platform"))
        self.assertGreater(len(items), 0)



class SdistManifestTests(unittest.TestCase):
    """A wheel is what `pip install` gets. An sdist is what a distro
    packager, a mirror and `pip install --no-binary :all:` get, and it was a
    different, smaller thing.

    Measured before MANIFEST.in existed: the sdist carried README.md,
    LICENSE, pyproject.toml, the package -- and every `tests/test_*.py` with
    **none** of `tests/fixtures/`. So it shipped a suite that cannot import,
    and dropped NOTICE, the file recording that the vendored corpora are
    redistributed under their own licences.

    MANIFEST.in is what decides an sdist's contents, so it is what is
    asserted here. `SdistContentsTests` below builds one for real where a
    PEP 517 front end is available; this class needs nothing and always runs.
    """

    @classmethod
    def setUpClass(cls):
        path = ROOT / "MANIFEST.in"
        cls.exists = path.is_file()
        raw = path.read_text(encoding="utf-8") if cls.exists else ""
        # Directives only. Half this file is a comment explaining what was
        # missing, and a commented-out `recursive-include` satisfied a naive
        # substring check while shipping nothing.
        cls.text = "\n".join(line for line in raw.splitlines()
                              if line.strip() and not line.startswith("#"))

    def test_it_exists(self):
        self.assertTrue(self.exists, "no MANIFEST.in: the sdist is whatever "
                                     "setuptools guesses, which was wrong")

    def test_it_ships_the_files_a_redistributor_needs(self):
        # Exact directives. `include tests/fixtures/real/NOTICE` contains the
        # string "NOTICE" and does not ship the top-level one.
        directives = set(self.text.splitlines())
        for name in ("NOTICE", "CHANGELOG.md", "PRIVACY.md"):
            with self.subTest(name=name):
                self.assertIn(f"include {name}", directives)

    def test_it_ships_the_fixtures_whenever_it_ships_the_tests(self):
        """The invariant, not the spelling. A test suite without its fixtures
        is worse than no test suite: it looks runnable and is not."""
        if "recursive-include tests *.py" not in self.text.splitlines():
            self.skipTest("the sdist no longer ships tests at all, which is "
                          "also a consistent answer")
        self.assertIn("recursive-include tests/fixtures", self.text)

    def test_it_ships_the_plugin_surface_and_the_docs(self):
        for fragment in ("docs", "commands", "skills", ".mcp.json"):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.text)

    def test_it_prunes_derived_directories(self):
        for name in ("demo-input", "demo-output", "build", "dist"):
            with self.subTest(name=name):
                self.assertIn(f"prune {name}", self.text)


_SETUPTOOLS_FLOOR_RE = re.compile(r"setuptools\s*>=\s*([0-9][0-9.]*)")


def _declared_setuptools_floor():
    """The setuptools version pyproject's `build-system` actually requires.

    Read out of the file rather than written down twice. A floor duplicated
    into a test is a floor that drifts, and it drifts silently: the gate goes
    on admitting a setuptools the build has already stopped supporting.
    """
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    found = _SETUPTOOLS_FLOOR_RE.search(text)
    return found.group(1) if found else None


def _version_tuple(raw):
    """Enough of a version parse to compare two release numbers.

    Not a PEP 440 implementation -- a suffix like `.dev0` contributes its
    digits and the comparison is still right either side of the floor, which
    is all this gate asks of it.
    """
    parts = []
    for piece in str(raw).split("."):
        digits = "".join(ch for ch in piece if ch.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def _require_buildable_setuptools(found, floor):
    """Skip unless the ambient setuptools can build *this* project.

    `_build_sdist` used to ask only whether setuptools imports. That is the
    wrong question. MEASURED 2026-09-30: a setuptools older than 61 does not
    read `[project]` out of pyproject.toml at all, so it builds this
    pyproject-only package as `UNKNOWN-0.0.0/`. `SdistContentsTests` then
    strips a root prefix that is not in the archive, gets an EMPTY name set,
    and every assertion in the class fails -- six failures, none of them
    naming the cause, where the README promises a single skip.

    The version is exactly what separates "this machine cannot build an
    sdist" from "our packaging is broken", and only the second should be
    loud. Takes both versions as arguments so the gate is testable on a
    machine that has no setuptools at all -- which is the machine this suite
    normally runs on.
    """
    if floor and _version_tuple(found) < _version_tuple(floor):
        raise unittest.SkipTest(
            f"setuptools {found} is older than the {floor} that "
            f"pyproject.toml's build-system requires, and an older one "
            f"builds this pyproject-only project as UNKNOWN-0.0.0, so the "
            f"sdist buildable here is not this project")


def _build_sdist(into: Path):
    """Build an sdist with the declared backend, or skip.

    `pip wheel` above can build a wheel because pip creates its own isolated
    build environment. pip has no equivalent for an sdist, and adding a PEP
    517 front end (`build`) would be a new test dependency this project does
    not have. So this uses setuptools directly when setuptools happens to be
    importable, and skips when it is not -- which is the case in the bare
    venv `make setup` creates.

    The skip names `setuptools`, the *backend* pyproject declares, because
    that is what is missing. It used to say "no PEP 517 front end
    importable", which is the other half of the build and is usually
    present: measured 2026-09-29 in the venv this suite runs in, `build`
    imports and `setuptools` does not. A reader following that message went
    to install something they already had.
    """
    try:
        import setuptools
        from setuptools import build_meta
    except ImportError:  # pragma: no cover - environment dependent
        raise unittest.SkipTest(
            "setuptools is not importable, and it is the PEP 517 build "
            "backend pyproject.toml declares, so no sdist can be built "
            "here; MANIFEST.in is asserted statically by SdistManifestTests")
    _require_buildable_setuptools(
        getattr(setuptools, "__version__", "0"), _declared_setuptools_floor())
    # Copy the tree first. setuptools folds an existing `*.egg-info/
    # SOURCES.txt` back into a new sdist, so building in place made this
    # test pass against a MANIFEST.in that had stopped listing the fixtures
    # -- it was reading the previous build's answer.
    skip = {".git", ".venv", "node_modules", "build", "dist", "demo-input",
            "demo-output", "__pycache__", ".superpowers", ".pytest_cache"}
    pristine = into / "src"
    shutil.copytree(
        ROOT, pristine,
        ignore=lambda d, names: [n for n in names
                                 if n in skip or n.endswith(".egg-info")])
    cwd = os.getcwd()
    try:
        os.chdir(pristine)
        return into / build_meta.build_sdist(str(into))
    finally:
        os.chdir(cwd)


class SdistContentsTests(unittest.TestCase):
    """The real thing, where it can be built."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        archive = _build_sdist(Path(cls._tmp.name))
        with tarfile.open(archive) as tar:
            entries = tar.getnames()
        root = f"fabric_aidp_migrator-{__import__('fabric_aidp').__version__}/"
        cls.names = {n[len(root):] for n in entries if n.startswith(root)}
        if not cls.names:
            # The environmental cause of this is gated above. Anything that
            # reaches here is the repository's own problem and should say so:
            # an empty name set used to make all six assertions below fail
            # separately, with nothing pointing at the missing root.
            raise AssertionError(
                f"the sdist has no {root} directory, so there is nothing to "
                f"assert about; its roots are "
                f"{sorted({n.split('/')[0] for n in entries})}")

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_attribution_files_ship(self):
        for name in ("LICENSE", "NOTICE", "README.md", "CHANGELOG.md",
                     "PRIVACY.md"):
            with self.subTest(name=name):
                self.assertIn(name, self.names)

    def test_the_shipped_tests_have_their_fixtures(self):
        tests = {n for n in self.names
                 if n.startswith("tests/") and n.endswith(".py")}
        fixtures = {n for n in self.names if n.startswith("tests/fixtures/")}
        self.assertTrue(tests)
        self.assertTrue(fixtures, "the sdist ships %d test modules and no "
                                  "fixtures" % len(tests))
        # The three corpora the suite's ratchets are sized against.
        for corpus in ("corpus", "notebooks", "pipelines", "warehouse"):
            with self.subTest(corpus=corpus):
                self.assertTrue(
                    any(n.startswith(f"tests/fixtures/real/corpora/{corpus}/")
                        for n in self.names))

    def test_the_third_party_notices_travel_with_the_third_party_files(self):
        self.assertIn("tests/fixtures/real/corpora/NOTICE", self.names)
        self.assertIn("tests/fixtures/real/THIRD_PARTY_LICENSES.md", self.names)

    def test_the_demo_can_be_staged_from_an_unpacked_sdist(self):
        """`make demo` is the documented first command, and it runs
        `scripts/stage_demo_input.py` over the vendored corpora. Neither was
        in the sdist."""
        self.assertIn("scripts/stage_demo_input.py", self.names)
        self.assertIn("demo.sh", self.names)
        self.assertIn("Makefile", self.names)

    def test_derived_directories_do_not_ship(self):
        for prefix in ("demo-input/", "demo-output/", "build/", "dist/"):
            with self.subTest(prefix=prefix):
                self.assertEqual(
                    [n for n in self.names if n.startswith(prefix)], [])

    def test_node_modules_does_not_ship(self):
        self.assertEqual([n for n in self.names if "node_modules" in n], [])


class SourceWarningTests(unittest.TestCase):
    """Every shipped module compiles with no SyntaxWarning.

    An invalid escape in a docstring -- `\\/` was the one that got through --
    is a SyntaxWarning today and a SyntaxError in a future Python, and it
    prints on every import in the meantime. Measured before this test
    existed: `fabric_notebook_to_spark.py` emitted one on every CLI run.
    """

    def test_no_module_emits_a_syntax_warning(self):
        root = pathlib.Path(__file__).resolve().parent.parent / "fabric_aidp"
        offenders = []
        for path in sorted(root.rglob("*.py")):
            if "node_modules" in path.parts or "fixtures" in path.parts:
                continue
            source = path.read_text(encoding="utf-8")
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", SyntaxWarning)
                compile(source, str(path), "exec")
            for entry in caught:
                if issubclass(entry.category, SyntaxWarning):
                    offenders.append(f"{path.name}:{entry.lineno}: {entry.message}")
        self.assertEqual(offenders, [], "\n".join(offenders))



class TestFooterTests(unittest.TestCase):
    """`unittest.main()` must be the last statement in a test file.

    Put it mid-file and `python tests/test_x.py` silently runs only the
    classes above it, while `-m unittest` runs them all. Measured before
    this test existed: eight files hid 98 tests between them --
    `test_tsql_rules.py` ran 141 directly and 172 under `-m unittest`.
    Nothing failed; the tests just did not run, which is worse.
    """

    def test_unittest_main_is_the_last_statement(self):
        root = pathlib.Path(__file__).resolve().parent
        pattern = re.compile(r'^if __name__ == ["\']__main__["\']:', re.M)
        offenders = []
        for path in sorted(root.glob("test_*.py")):
            source = path.read_text(encoding="utf-8")
            match = pattern.search(source)
            if match is None:
                continue
            tail = source[match.end():]
            # Everything after the guard must be its own indented body.
            for line in tail.splitlines():
                if line.strip() and not line[:1].isspace():
                    offenders.append(f"{path.name}: {line.strip()[:60]}")
                    break
        self.assertEqual(offenders, [], "\n".join(offenders))


class SkipMessageNamesTheMissingThingTests(unittest.TestCase):
    """`_build_sdist` skipped with "no PEP 517 front end importable".

    That names the wrong half of the build. A front end (`build`, `pip`) is
    the thing that *drives* a PEP 517 build; the backend is what pyproject's
    `build-backend` points at, here `setuptools.build_meta`. The skip fires
    on `from setuptools import build_meta`, which is the backend.

    MEASURED 2026-09-29 in the venv this suite runs in:

        build        importable
        setuptools   NOT importable (No module named 'setuptools')

    So the message told a reader to go and install a PEP 517 front end that
    was already there, while the module actually missing went unnamed.

    Read statically: the point is what the string says, and running the skip
    needs a machine that is missing something.
    """

    @classmethod
    def setUpClass(cls):
        source = (ROOT / "tests" / "test_packaging.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        cls.func = next(n for n in ast.walk(tree)
                        if isinstance(n, ast.FunctionDef) and n.name == "_build_sdist")

    def _imported_modules(self):
        names = set()
        for node in ast.walk(self.func):
            if isinstance(node, ast.ImportFrom) and node.module:
                names.add(node.module.split(".")[0])
            elif isinstance(node, ast.Import):
                names.update(a.name.split(".")[0] for a in node.names)
        return names

    def _skip_message(self):
        for node in ast.walk(self.func):
            if not isinstance(node, ast.Raise) or node.exc is None:
                continue
            call = node.exc
            if isinstance(call, ast.Call) and node.exc.args:
                arg = call.args[0]
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    return arg.value
                if isinstance(arg, ast.JoinedStr):
                    return "".join(v.value for v in arg.values
                                   if isinstance(v, ast.Constant))
        self.fail("_build_sdist no longer raises SkipTest with a message")

    def test_the_skip_names_the_module_whose_absence_causes_it(self):
        message = self._skip_message().lower()
        missing = self._imported_modules()
        self.assertTrue(missing, "_build_sdist imports nothing to fail on")
        for name in sorted(missing):
            with self.subTest(module=name):
                self.assertIn(
                    name, message,
                    f"_build_sdist skips when `{name}` will not import, and "
                    f"says {self._skip_message()!r} -- which does not name "
                    f"it. A skip message that names the wrong missing thing "
                    f"sends the reader to install something they have.")

    def test_it_does_not_blame_a_front_end_for_a_missing_backend(self):
        message = self._skip_message().lower()
        self.assertNotIn(
            "no pep 517 front end", message,
            "the missing module is the build *backend* pyproject declares, "
            "not a front end: this venv has `build` and has no `setuptools`")


class EnvironmentalFailuresDegradeToSkipsTests(unittest.TestCase):
    """The README promises a skip when "a distribution cannot be built here".

    Two paths returned something else. Both were reproduced before the fix,
    MEASURED 2026-09-30:

        pip wheel stalls on network   -> subprocess.TimeoutExpired out of
                                         setUpClass = one class ERROR
        ambient setuptools older      -> sdist root is `UNKNOWN-0.0.0/`, the
        than pyproject's floor           name set comes out EMPTY, and all
                                         six SdistContentsTests assertions
                                         FAIL with no hint of the cause

    This was carried on the issue as "environmental, identical before this
    work". It is not environmental: the first path never caught a timeout and
    the second only ever asked whether setuptools *imports*. Neither needs a
    bare 3.9 interpreter to pin, so neither is pinned with one.
    """

    def test_a_stalled_pip_wheel_skips_rather_than_errors(self):
        stall = subprocess.TimeoutExpired(cmd="pip wheel", timeout=600)
        with mock.patch("subprocess.run", side_effect=stall):
            with self.assertRaises(unittest.SkipTest) as caught:
                _build_wheel(Path(tempfile.gettempdir()))
        self.assertIn("wheel", str(caught.exception).lower())

    def test_a_pip_wheel_that_merely_fails_still_skips(self):
        """The pre-existing path, kept honest alongside the new one."""
        done = subprocess.CompletedProcess([], 1, stdout="", stderr="no network")
        with mock.patch("subprocess.run", return_value=done):
            with self.assertRaises(unittest.SkipTest):
                _build_wheel(Path(tempfile.gettempdir()))

    def test_the_floor_is_read_from_pyproject_and_is_really_there(self):
        """A `None` floor turns the gate below into a silent no-op, so the
        regex missing its target has to be a failure and not a shrug."""
        floor = _declared_setuptools_floor()
        self.assertIsNotNone(
            floor, "no `setuptools>=N` found in pyproject.toml's "
                   "build-system.requires, which disables the version gate")
        requires = re.search(r"requires\s*=\s*\[[^\]]*\]",
                             (ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertIsNotNone(requires)
        self.assertIn(floor, requires.group(0))

    def test_a_setuptools_below_the_floor_skips_and_names_both_versions(self):
        with self.assertRaises(unittest.SkipTest) as caught:
            _require_buildable_setuptools("58.0.0", "77")
        message = str(caught.exception)
        for fragment in ("58.0.0", "77", "sdist"):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, message)

    def test_a_setuptools_at_or_above_the_floor_does_not_skip(self):
        for found in ("77", "77.0.0", "80.9.0", "121.0"):
            with self.subTest(found=found):
                _require_buildable_setuptools(found, "77")

    def test_an_absent_floor_does_not_invent_a_skip(self):
        """Better to attempt the build and report what happened than to
        skip on a version comparison against nothing."""
        _require_buildable_setuptools("58.0.0", None)

    def test_the_version_compare_orders_releases_not_strings(self):
        self.assertLess(_version_tuple("9.0"), _version_tuple("77"))
        self.assertLess(_version_tuple("77.0"), _version_tuple("77.0.1"))
        self.assertEqual(_version_tuple("77"), _version_tuple("77"))

    def test_an_sdist_with_no_matching_root_says_so(self):
        """The residual after the gate is a repository problem, and the
        empty-name-set shape hid it behind six unrelated assertions."""
        tree = ast.parse((ROOT / "tests" / "test_packaging.py")
                         .read_text(encoding="utf-8"))
        suite = next(n for n in ast.walk(tree)
                     if isinstance(n, ast.ClassDef)
                     and n.name == "SdistContentsTests")
        setup = next(n for n in suite.body
                     if isinstance(n, ast.FunctionDef)
                     and n.name == "setUpClass")
        raised = {getattr(n.exc.func, "id", None)
                  for n in ast.walk(setup)
                  if isinstance(n, ast.Raise) and isinstance(n.exc, ast.Call)}
        self.assertIn(
            "AssertionError", raised,
            "SdistContentsTests.setUpClass no longer fails loudly on an "
            "unmatched sdist root, so an empty name set is back to failing "
            "six assertions instead of naming the cause")


if __name__ == "__main__":
    unittest.main()
