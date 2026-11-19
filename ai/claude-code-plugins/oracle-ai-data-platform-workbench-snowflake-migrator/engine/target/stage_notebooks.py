"""Build the data-plane stages as self-contained AIDP notebooks.

WHY NOTEBOOKS, AND WHY .ipynb SPECIFICALLY. AIDP types a workspace object by
its EXTENSION, not by the `--type` you upload it with: a `.py` is stored as
`FILE` even when created with `--type NOTEBOOK`, and only `.ipynb` becomes a
`NOTEBOOK`. A job task is a `NOTEBOOK_TASK` pointing at a NOTEBOOK, so the
data plane has to be `.ipynb` or it cannot be run as a job at all. Verified
live 2026-09-19 by uploading both shapes and reading the listing back.

WHY SELF-CONTAINED. The previous design uploaded four `.py` scripts plus a
shared `snowmig_source.py`, then generated a one-cell driver notebook per job
that `runpy`-ed the script off the `/Workspace` mount. That indirection cost
three things: the code a user opens in the console is not the code that runs,
a mount-path assumption sits between the job and its logic, and an editable
parameter lives in a generated wrapper rather than beside the work. Each
stage is now ONE notebook carrying its own parameters, its own copy of the
shared helpers, and its own stage logic.

WHY GENERATED RATHER THAN HAND-WRITTEN. The stage logic is long and tested;
hand-maintaining five copies of the shared helpers inside four notebooks is
how they drift. The canonical Python lives once in `engine/dataplane/`, and
this module assembles it into notebooks. The generated `.ipynb` ARE committed
-- they are the artifact that ships and the thing a reviewer reads -- but
they are regenerated, never edited by hand.
"""
from __future__ import annotations

import ast
import json
import pathlib
import re

__all__ = ["DIAGNOSE_NOTEBOOK_NAME", "DIAGNOSE_SOURCE_NAME", "STAGES",
           "param_spellings",
           "StageSpec", "build_diagnose_notebook", "build_stage_notebook",
           "check_stage_params", "dataplane_dir", "declared_stage_params",
           "write_stage_notebooks"]

# The shared helper module every stage needs, inlined into each notebook.
SHARED_SOURCE_NAME = "snowmig_source.py"

# The environment diagnosis is generated like the stages but is NOT a stage:
# no job runs it, it has no argparse `main()`, and a human reads its verdicts
# cell by cell. Its source is split on `# %%` markers -- one cell per check,
# `# %% [markdown]` for prose -- its module docstring is the notebook header,
# and its parameters are edited in place with this run's coordinates. It used
# to be a hand-maintained `.ipynb` that imported `snowmig_source` off the
# mount (never uploaded there any more) and echoed the config with a
# top-level-only redaction, which printed a nested `snowflake:` block whole.
DIAGNOSE_SOURCE_NAME = "diagnose_environment.py"
DIAGNOSE_NOTEBOOK_NAME = "diagnose_environment.ipynb"
# provision's override keys -> the parameter each one sets. The rest of what
# provision knows (target catalog, reports dir) is not a diagnosis input.
DIAGNOSE_PARAMS = {"source-config": "CONFIG_PATH",
                   "source-catalog": "EXTERNAL_CATALOG",
                   "session-schema": "SESSION_SCHEMA"}
_CELL_MARK = re.compile(r"^# %% ?(.*)$", re.MULTILINE)

# `from snowmig_source import (...)` plus the sys.path line that made it
# resolvable off the mount. Both are meaningless once the helpers are inlined
# in the notebook itself, so they are stripped -- and the strip is ASSERTED,
# because a silent miss would leave a notebook importing a module that is no
# longer uploaded.
_IMPORT_BLOCK = re.compile(
    r"^sys\.path\.insert\(0, str\(pathlib\.Path\(__file__\)[^\n]*\n"
    r"from snowmig_source import \([^)]*\)\n",
    re.MULTILINE)

# `if __name__ == "__main__": sys.exit(main())`. A notebook cell that raises
# SystemExit is reported as a FAILED task even when the work succeeded -- a
# full discovery once came back failed for exactly that reason -- so the run
# cell calls main() and inspects the returned code instead.
# An actual import of the helper module, as opposed to prose mentioning it.
_IMPORTS_SHARED = re.compile(
    r"^\s*(?:from\s+snowmig_source\s+import|import\s+snowmig_source)\b",
    re.MULTILINE)

_MAIN_GUARD = re.compile(
    r"\n\nif __name__ == [\"']__main__[\"']:\n    sys\.exit\(main\(\)\)\n?$")


class StageSpec:
    """One data-plane stage: its source, its job, its editable parameters.

    `params` declares EVERY flag the stage's argparse accepts, no more and
    no fewer (a test derives them from the real parser). A default of
    `False` marks a switch. `lists` are flags taking several values after
    one flag (`nargs="*"`); `repeated` are flags given once per value
    (`action="append"`). The kinds matter because a value arrives from
    `provision --stage-param` as text: `counts=true` has to become a bare
    `--counts`, not `--counts true`, which argparse rejects. `choices` are
    the argparse choices per flag, recorded because one name can mean
    different things in different stages: `mode` is ddl-plan/ctas/manifest
    in 01 and skip-existing/append/overwrite in 02, and a value only one of
    them accepts would otherwise pass provision and fail on the cluster.
    """

    def __init__(self, *, key: str, source: str, job: str, title: str,
                 blurb: str, params: dict[str, object],
                 required: tuple[str, ...] = (),
                 lists: tuple[str, ...] = (),
                 repeated: tuple[str, ...] = (),
                 choices: dict[str, tuple[str, ...]] | None = None):
        self.key = key
        self.source = source
        self.job = job
        self.title = title
        self.blurb = blurb
        self.params = params
        self.required = required
        self.lists = lists
        self.repeated = repeated
        self.choices = dict(choices or {})

    @property
    def notebook_name(self) -> str:
        return self.source.replace(".py", ".ipynb")


# Parameter DEFAULTS only -- never an estate's real names. A value that
# identifies one customer's environment does not belong in a plugin that
# ships to everyone, so anything site-specific defaults to None and the
# provisioner fills it in from the run's own coordinates.
# snowmig_source.SOURCE_MODES, restated: this module assembles the data-plane
# sources as text and does not import them. The argparse test pins the two.
_SOURCE_MODES = ("connector", "external-catalog")

STAGES: tuple[StageSpec, ...] = (
    StageSpec(
        key="discover", source="00_discover_snowflake.py",
        job="snowmig_00_discover",
        title="S6 · Discover the Snowflake estate",
        blurb=(
            "Reads the source through the AIDP connector and writes "
            "`discovery_manifest.json`. Costs TWO `INFORMATION_SCHEMA` "
            "queries for the whole database, which is what makes a "
            "hundred-thousand-table estate feasible; a `DESCRIBE` per object "
            "does not scale and is not the path.\n\n"
            "**Read-only against Snowflake.** The transport refuses any verb "
            "that is not SELECT/SHOW/DESCRIBE/DESC/WITH/EXPLAIN before it "
            "reaches the source."),
        params={"source-mode": "connector", "source-config": None,
                "source-catalog": None, "session-schema": None,
                "schemas": None, "exclude-schemas": None,
                "reports-dir": None, "backup-dir": None, "output-dir": None,
                "force": False},
        lists=("schemas", "exclude-schemas"),
        choices={"source-mode": _SOURCE_MODES},
    ),
    StageSpec(
        key="structure", source="01_create_structure.py",
        job="snowmig_01_structure",
        title="S10 · Create the target structure",
        blurb=(
            "Creates the schemas and the empty Delta tables in the INTERNAL "
            "target catalog, one schema per run. **Structure only — no rows "
            "move.** Every table arrives empty.\n\n"
            "Runs on AIDP compute, where each table is read back after its "
            "CREATE and, in the default `ddl-plan` mode, compared with the "
            "approved plan column by column. `parallel` tables "
            "are created at once (default 8; `--mode ctas`: 1); views "
            "follow every table, one at a time, in the plan's order."),
        # `mode` is `ddl-plan`, matching the stage's own argparse default and
        # runbook S10 ("reading the approved plan"). It used to ship
        # `manifest`, which CANNOT work alongside the `connector` source-mode
        # directly above it: a connector-built manifest records SNOWFLAKE
        # types, and Delta rejects them verbatim. The shipped default pair was
        # therefore guaranteed to fail -- every table refused with "use
        # --mode ddl-plan" -- on a first, unmodified run.
        params={"source-mode": "connector", "source-config": None,
                "source-catalog": None, "target-catalog": None,
                "schema": None, "target-schema": None, "mode": "ddl-plan",
                "ddl-plan": None, "reports-dir": None, "output-dir": None,
                "parallel": None, "dry-run": False, "force": False},
        required=("target-catalog",),
        repeated=("schema",),
        choices={"source-mode": _SOURCE_MODES,
                 "mode": ("ddl-plan", "ctas", "manifest")},
    ),
    StageSpec(
        key="copy_schema", source="02_copy_schema.py",
        job="snowmig_02_copy_schema",
        title="Copy one schema's data",
        blurb=(
            "Moves rows for a single schema. **Not part of S1–S12** — the "
            "migration registers this notebook and never runs it. Moving "
            "data is a later decision the customer makes, with the notebook "
            "already sitting here.\n\n"
            "One notebook per SCHEMA, never per table: each job run has its "
            "own startup time, so one run per schema pays it once for every "
            "table in that schema. Within the schema, `parallel` "
            "tables are copied at once (default 8; 1 = one after "
            "another)."),
        params={"source-mode": "connector", "source-config": None,
                "source-catalog": None, "target-catalog": None,
                "schema": None, "target-schema": None, "ddl-plan": None,
                "tables": None,
                "mode": "skip-existing", "verify": "counts",
                "reports-dir": None, "output-dir": None, "dry-run": False,
                "retries": None, "retry-base-delay": None,
                "retry-multiplier": None, "parallel": None, "force": False},
        required=("target-catalog", "schema"),
        lists=("tables",),
        choices={"source-mode": _SOURCE_MODES,
                 "mode": ("skip-existing", "append", "overwrite"),
                 "verify": ("counts", "counts+sums")},
    ),
    StageSpec(
        key="reconcile", source="03_reconcile.py",
        job="snowmig_03_reconcile",
        title="Reconcile source against target",
        blurb=(
            "Compares what the target holds against what discovery recorded "
            "and reports the verdict per table. Read-only on both ends."),
        params={"target-catalog": None, "ddl-plan": None, "reports-dir": None, "output-dir": None,
                "counts": False},
        required=("target-catalog",),
    ),
)


_TRUE = ("true", "yes", "on", "1")
_FALSE = ("false", "no", "off", "0", "")


# A drive-letter path. A stage notebook runs on the AIDP cluster (Linux), so
# no such path is ever right there -- and it is exactly what Git Bash makes
# of `/Workspace/...` (MSYS path conversion: live 2026-09-29, reports-dir
# became `C:/Program Files/Git/Workspace/...`, discovery wrote its manifest
# to that local path on the cluster, and the next stage could not find it).
_WINDOWS_PATH = re.compile(r"^\s*[A-Za-z]:[\\/]")


def _refuse_windows_path(key: str, value: object) -> None:
    if isinstance(value, str) and _WINDOWS_PATH.match(value):
        raise ValueError(
            f"--stage-param {key}={value!r}: a Windows path cannot be right "
            f"for a stage that runs on the AIDP cluster. If you typed a "
            f"/Workspace/... path in Git Bash, Git Bash rewrote it (MSYS path "
            f"conversion): re-run with MSYS_NO_PATHCONV=1 set, or write the "
            f"value as //Workspace/...")


def _coerce(stage: StageSpec, key: str, value: object) -> object:
    """A --stage-param value, as text, into the literal PARAMS holds.

    A switch takes true/false and nothing else: `--counts true` is an
    argparse error on the cluster, minutes into a job run, and a value like
    `maybe` is a typo that must not be guessed either way. A list flag
    takes a comma-separated value. A flag with choices takes one of them,
    for the same reason as the switch. Anything else is passed as written.
    """
    if value is None or isinstance(value, bool):
        return value
    _refuse_windows_path(key, value)
    if isinstance(stage.params.get(key), bool):
        text = str(value).strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
        raise ValueError(
            f"--stage-param {key}={value!r}: `{key}` is a switch of "
            f"{stage.source}; give true or false")
    if key in stage.lists or key in stage.repeated:
        items = (value if isinstance(value, (list, tuple))
                 else str(value).split(","))
        value = [str(v).strip() for v in items if str(v).strip()]
    allowed = stage.choices.get(key)
    if allowed is not None:
        bad = [v for v in (value if isinstance(value, list) else [value])
               if str(v) not in allowed]
        if bad:
            raise ValueError(
                f"--stage-param {key}={bad[0]!r}: `{key}` of {stage.source} "
                f"takes one of {', '.join(allowed)}")
    return value


def _split_name(name: str) -> tuple[str | None, str]:
    """`copy_schema.mode` -> ("copy_schema", "mode"); `mode` -> (None, "mode").

    A stage-qualified name is how one value reaches ONE stage: a flag name
    never contains a dot, a stage key never does either.
    """
    stage, dot, flag = name.partition(".")
    return (stage, flag) if dot else (None, name)


def _for_stage(stage: StageSpec, overrides: dict[str, object]
               ) -> dict[str, object]:
    """The overrides that reach `stage`: every unqualified name it declares,
    then `<stage.key>.<name>` on top -- a qualified value wins for its own
    stage and reaches no other."""
    out: dict[str, object] = {}
    qualified: dict[str, object] = {}
    for name, value in overrides.items():
        prefix, flag = _split_name(name)
        if flag not in stage.params:
            continue
        if prefix is None:
            out[flag] = value
        elif prefix == stage.key:
            qualified[flag] = value
    out.update(qualified)
    return out


def param_spellings(name: str) -> list[str]:
    """Every spelling of a stage parameter a workflow may carry, in lookup
    order: dry-run, dry_run, dryRun, dryrun and the upper-case forms. The
    generated PARAMS cell reads these (its `_spellings` is this function,
    restated as notebook text -- a test pins the two); `run` checks a job's
    task parameters against them before submitting."""
    parts = name.split("-")
    snake = "_".join(parts)
    camel = parts[0] + "".join(p[:1].upper() + p[1:] for p in parts[1:])
    flat = "".join(parts)
    return list(dict.fromkeys((name, snake, camel, flat, name.upper(),
                               snake.upper(), flat.upper())))


def declared_stage_params() -> dict[str, list[str]]:
    """Every name some stage declares -> the stage sources that declare it."""
    names: dict[str, list[str]] = {}
    for stage in STAGES:
        for key in stage.params:
            names.setdefault(key, []).append(stage.source)
    return names


def check_stage_params(params: dict[str, object],
                       copy_schemas=()) -> None:
    """Refuse a --stage-param no stage can take, before anything is called.

    A name no stage declared used to be dropped per stage without a word,
    so `tables=ORDERS dry-run=true mode=overwrite` produced a copy notebook
    carrying only the overwrite. Refusing is the only honest outcome for a
    scope flag: dropping one reads as applied.

    An unqualified name goes to EVERY stage that declares it, so its value
    has to suit every one of them. `mode=overwrite` does not: 01 declares
    `mode` too, with other choices, and used to receive `--mode overwrite`
    and fail argparse when its job ran. `schema=A,B` does not either: two
    schemas to 01, the literal `A,B` to 02. Such a value is refused with
    the `<stage>.<name>` form that sends it to the one stage meant.

    With per-schema copy jobs (`copy_schemas`, from the approved plan) the
    ONE 02_copy_schema notebook backs every copy job, and each job passes
    its `schema` as a task parameter, which wins over the PARAMS literal.
    So a baked copy `schema` changes nothing the copy does -- it would only
    narrow 01 and be recorded as written -- and a baked `tables` narrows
    EVERY schema's copy job at once. Both are refused.
    """
    by_key = {s.key: s for s in STAGES}
    for name, value in params.items():
        _refuse_windows_path(name, value)
    _check_against_copy_jobs(params, list(copy_schemas or ()))
    declared = declared_stage_params()
    unknown = sorted(k for k in params
                     if _split_name(k)[0] is None and k not in declared)
    if unknown:
        raise ValueError(
            "--stage-param " + ", ".join(unknown) + ": no stage notebook "
            "declares " + ("that name" if len(unknown) == 1 else "those names")
            + ", so it would reach nothing. Declared names -- "
            + "; ".join(f"{s.notebook_name}: {', '.join(s.params)}"
                        for s in STAGES)
            + ". Prefix a name with a stage (" + ", ".join(by_key)
            + ") to send it to that stage only, e.g. copy_schema.mode.")
    for name in params:
        prefix, flag = _split_name(name)
        if prefix is None:
            continue
        stage = by_key.get(prefix)
        if stage is None:
            raise ValueError(
                f"--stage-param {name}: there is no stage `{prefix}`. The "
                f"stages are " + ", ".join(by_key) + ".")
        if flag not in stage.params:
            raise ValueError(
                f"--stage-param {name}: {stage.source} does not declare "
                f"`{flag}`, so it would reach nothing. It declares "
                + ", ".join(stage.params) + ".")
    for name, value in params.items():
        if _split_name(name)[0] is not None:
            continue
        # The stages this unqualified value actually reaches: a stage given
        # its own `<stage>.<name>` is not one of them.
        reached = [s for s in STAGES if name in s.params
                   and f"{s.key}.{name}" not in params]
        if len(reached) < 2:
            continue
        items = (value if isinstance(value, (list, tuple))
                 else str(value).split(","))
        several = len([v for v in items if str(v).strip()]) > 1
        rejecting, fitting = [], []
        for stage in reached:
            try:
                _coerce(stage, name, value)
            except ValueError as exc:
                # The reason only; the name and value lead the message.
                rejecting.append(str(exc).split(": ", 1)[-1])
                continue
            if several and not (name in stage.lists
                                or name in stage.repeated):
                rejecting.append(
                    f"{stage.source} takes ONE `{name}`, so {value!r} "
                    f"would reach it as that literal text")
                continue
            fitting.append(stage)
        if rejecting:
            raise ValueError(
                f"--stage-param {name}={value!r} goes to every stage that "
                f"declares `{name}` ("
                + ", ".join(s.source for s in reached) + "), and "
                + "; ".join(rejecting) + ". Name the stage it is meant for: "
                + (" or ".join(f"{s.key}.{name}={value}" for s in fitting)
                   or f"<stage>.{name}=<value>") + ".")
    for stage in STAGES:
        for key, value in _for_stage(stage, params).items():
            _coerce(stage, key, value)


def _check_against_copy_jobs(params: dict[str, object],
                             copy_schemas: list[str]) -> None:
    if not copy_schemas:
        return
    jobs = ", ".join(copy_schemas)
    for name in ("schema", "copy_schema.schema"):
        if name in params:
            raise ValueError(
                f"--stage-param {name}={params[name]!r}: every per-schema "
                f"copy job ({jobs}) passes its own `schema` as a task "
                f"parameter, and a task parameter wins over the PARAMS "
                f"literal, so a baked copy `schema` would change nothing "
                f"the copy does"
                + (" -- and, unqualified, it would still narrow "
                   "01_create_structure" if name == "schema" else "")
                + ". To narrow the structure stage write "
                f"structure.schema=<value>; to copy fewer schemas, narrow "
                f"the approved plan and re-push.")
    if len(copy_schemas) < 2:
        return
    for name in ("tables", "copy_schema.tables"):
        if name in params:
            raise ValueError(
                f"--stage-param {name}={params[name]!r}: the one "
                f"02_copy_schema notebook backs every per-schema copy job "
                f"({jobs}), so a baked `tables` would narrow every "
                f"per-schema copy job to those names and leave the rest of "
                f"each schema uncopied. Narrow the approved plan instead, "
                f"or set `tables` on the one job's task in the console.")


def dataplane_dir() -> pathlib.Path:
    """Where the canonical stage sources live, relative to this file."""
    return pathlib.Path(__file__).resolve().parent.parent / "dataplane"


def _md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": [text]}


def _code(text: str) -> dict:
    return {"cell_type": "code", "execution_count": None, "metadata": {},
            "outputs": [], "source": [text]}


def _strip_shared_import(source: str, stage: str) -> tuple[str, bool]:
    """Remove the shared-module import. Returns (source, needs_helpers).

    Not every stage uses the helpers -- `03_reconcile` reads only the target
    -- so an absent import is fine. What is NOT fine is a stage that still
    mentions `snowmig_source` after the strip: that notebook would import a
    module it does not ship, and it would fail at run time on the cluster
    rather than here.
    """
    stripped, count = _IMPORT_BLOCK.subn("", source)
    # Only an IMPORT matters. Prose that merely names the helper file -- a
    # docstring, an argparse help string -- is fine and must not trip this.
    if not count and not _IMPORTS_SHARED.search(source):
        return stripped, False
    if _IMPORTS_SHARED.search(stripped):
        raise ValueError(
            f"{stage}: `snowmig_source` is still referenced after the import "
            f"block was stripped, so this notebook would import a module it "
            f"does not ship. Fix the pattern rather than emitting a notebook "
            f"that cannot run.")
    if not count:
        raise ValueError(
            f"{stage}: imports `snowmig_source` but its import block did "
            f"not match the expected shape, so nothing was stripped.")
    return stripped, True


def _strip_main_guard(source: str, stage: str) -> str:
    stripped, count = _MAIN_GUARD.subn("\n", source)
    if not count:
        raise ValueError(
            f"{stage}: no `if __name__ == '__main__': sys.exit(main())` "
            f"guard found. Left in place it raises SystemExit, which AIDP "
            f"reports as a FAILED task even when the stage succeeded.")
    return stripped


def _params_cell(stage: StageSpec,
                 overrides: dict[str, object] | None = None) -> str:
    lines = [
        "# ── PARAMETERS ─────────────────────────────────────────────────",
        "# Edit these, then run the notebook top to bottom. Every value is a",
        "# plain Python literal: `None` means the flag is not passed at all,",
        "# and `True` means a bare switch is passed.",
        "#",
        "# Scope and mode are INPUTS. To migrate less, change a value here --",
        "# never edit the stage logic below to make it cover less.",
        "PARAMS = {",
    ]
    merged = dict(stage.params)
    # Only parameters the stage actually declares: a flag it does not
    # accept would make argparse reject the whole run. provision() refuses
    # an explicit name no stage declares before it gets here
    # (check_stage_params); what is filtered here is a derived coordinate
    # that only some other stage takes, or `<other stage>.<name>`.
    for key, value in _for_stage(stage, overrides or {}).items():
        if value is not None:
            merged[key] = _coerce(stage, key, value)
    for key, value in merged.items():
        note = "  # REQUIRED" if key in stage.required else ""
        lines.append(f"    {key!r}: {value!r},{note}")
    switches = sorted(k for k, v in stage.params.items() if v is False)
    multi = sorted(set(stage.lists) | set(stage.repeated))
    lines += [
        "}",
        "",
        "# ── WORKFLOW PARAMETERS WIN ────────────────────────────────────",
        "# A job task's `parameters` ({name, value}) override the literals",
        "# above, by the same names (`schema`, `mode`, `dry-run`, ...). Read",
        "# with oidlUtils.parameters.getParameter -- resolved by the AIDP",
        "# runtime, never imported. The environment is only a fallback. This",
        "# is how ONE notebook serves one workflow per schema: each task",
        "# passes its own `schema`.",
        "import os as _os",
        "_MISS = '\\x00__no_parameter__'",
        f"_SWITCHES = {switches!r}",
        f"_MULTI = {multi!r}",
        f"_CHOICES = {dict(stage.choices)!r}",
        # The same words provision's --stage-param accepts (_coerce). Any
        # other text is refused: read as False it silently dropped a baked
        # `--dry-run`, and the dry run became a real write.
        f"_TRUE = {_TRUE!r}",
        f"_FALSE = {_FALSE!r}",
        "",
        "",
        "def _spellings(name):",
        "    # `dry-run` as an operator may type it in the console: dry-run,",
        "    # dry_run, dryRun, dryrun, and the upper-case forms. A spelling",
        "    # nobody reads is a silent default -- for dry-run, a real write.",
        "    parts = name.split('-')",
        "    snake = '_'.join(parts)",
        "    camel = parts[0] + ''.join(p[:1].upper() + p[1:] for p in parts[1:])",
        "    flat = ''.join(parts)",
        "    return list(dict.fromkeys((name, snake, camel, flat, name.upper(),",
        "                               snake.upper(), flat.upper())))",
        "",
        "",
        "def _workflow_param(name):",
        "    for n in _spellings(name):",
        "        try:",
        "            v = oidlUtils.parameters.getParameter(n, _MISS)  # noqa: F821",
        "        except NameError:  # outside AIDP: no oidlUtils",
        "            v = _MISS",
        "        if v != _MISS and v is not None and str(v).strip() != '':",
        "            return str(v).strip()",
        "    # The environment is a fallback only, by the plain spellings: a",
        "    # generic name (SCHEMA, DRYRUN) could be any stray cluster variable.",
        "    for n in dict.fromkeys((name, name.replace('-', '_'),",
        "                            name.replace('-', '_').upper())):",
        "        v = _os.environ.get(n)",
        "        if v is not None and str(v).strip() != '':",
        "            return str(v).strip()",
        "    return None",
        "",
        "",
        "PARAMS_FROM_WORKFLOW = {}",
        "for _key in list(PARAMS):",
        "    _v = _workflow_param(_key)",
        "    if _v is None:",
        "        continue",
        "    if _key in _SWITCHES:",
        "        if _v.lower() in _TRUE:",
        "            _v = True",
        "        elif _v.lower() in _FALSE:",
        "            _v = False",
        "        else:",
        "            # A typo is not guessed either way: read as False, a",
        "            # `dry-run` typo would turn a dry run into a real write.",
        "            raise ValueError(",
        "                f'workflow parameter {_key}={_v!r}: `{_key}` is a '",
        "                f'switch; give true or false')",
        "    elif _key in _MULTI:",
        "        _v = [p.strip() for p in _v.split(',') if p.strip()]",
        "    _bad = [c for c in (_v if isinstance(_v, list) else [_v])",
        "            if _key in _CHOICES and c not in _CHOICES[_key]]",
        "    if _bad:",
        "        raise ValueError(",
        "            f'workflow parameter {_key}={_bad[0]!r}: `{_key}` takes '",
        "            f\"one of {', '.join(_CHOICES[_key])}\")",
        "    PARAMS[_key] = PARAMS_FROM_WORKFLOW[_key] = _v",
        "print('from the workflow:', PARAMS_FROM_WORKFLOW)",
        "",
        "",
        "def _argv(params, repeated=()):",
        '    """PARAMS -> argv. None is omitted; True is a bare switch; a list',
        "    follows its flag, or repeats the flag per value for a name in",
        '    `repeated` (an append-style flag)."""',
        "    argv = []",
        "    for key, value in params.items():",
        "        if value is None or value is False:",
        "            continue",
        "        if value is True:",
        "            argv.append(f'--{key}')",
        "            continue",
        "        values = value if isinstance(value, (list, tuple)) else [value]",
        "        if key in repeated:",
        "            for v in values:",
        "                argv.extend((f'--{key}', str(v)))",
        "        else:",
        "            argv.append(f'--{key}')",
        "            argv.extend(str(v) for v in values)",
        "    return argv",
        "",
        "",
        (f"ARGV = _argv(PARAMS, repeated={stage.repeated!r})"
         if stage.repeated else "ARGV = _argv(PARAMS)"),
        "print('arguments:', ARGV)",
    ]
    missing = [k for k in stage.required]
    if missing:
        lines += [
            "",
            "_missing = [k for k in {!r} if not PARAMS.get(k)]".format(
                list(stage.required)),
            "if _missing:",
            "    raise ValueError(",
            "        f'set these PARAMS before running: {_missing}')",
        ]
    return "\n".join(lines)


_RUN_CELL = """\
# ── RUN ────────────────────────────────────────────────────────────
# main() RETURNS an exit code; it is not allowed to raise SystemExit here.
# A notebook cell that raises SystemExit is reported as a FAILED task even
# when the work succeeded, so the code is inspected and only a real failure
# is re-raised -- which keeps a genuinely failed stage failing.
code = main(ARGV)
print('exit code:', code, flush=True)
if code:
    raise RuntimeError(f'stage exited {code}')
"""


def build_stage_notebook(stage: StageSpec,
                         dataplane: pathlib.Path | None = None,
                         overrides: dict[str, object] | None = None) -> dict:
    """One self-contained `.ipynb` for `stage`: params, helpers, logic, run.

    `overrides` writes this run's coordinates into the PARAMS cell, so the
    notebook a user opens in the console already carries the right catalog
    and reports directory. Keys the stage does not declare are ignored
    rather than passed through -- argparse would reject an unknown flag and
    fail the whole run.
    """
    root = dataplane or dataplane_dir()
    body = (root / stage.source).read_text(encoding="utf-8")
    body, needs_helpers = _strip_shared_import(body, stage.source)
    body = _strip_main_guard(body, stage.source)

    header = (f"# {stage.title}\n\n{stage.blurb}\n\n"
              f"---\n\n"
              f"*Generated from `engine/dataplane/{stage.source}` by "
              f"`engine/target/stage_notebooks.py`. Regenerate with "
              f"`snowmig.py build-notebooks`; do not hand-edit — an edit here "
              f"is overwritten on the next build. Change the source instead.*")

    cells = [_md(header), _code(_params_cell(stage, overrides))]
    if needs_helpers:
        cells += [
            _md("## Shared source helpers\n\nInlined from "
                "`engine/dataplane/snowmig_source.py` so this notebook runs "
                "with nothing else uploaded beside it."),
            _code((root / SHARED_SOURCE_NAME).read_text(encoding="utf-8")),
        ]
    cells += [_md("## Stage logic"), _code(body), _code(_RUN_CELL)]
    return {"cells": cells,
            "metadata": {"snowmig": {"generated": True, "stage": stage.key,
                                     "source": stage.source,
                                     "job": stage.job},
                         "kernelspec": {"display_name": "Python 3",
                                        "language": "python",
                                        "name": "python3"},
                         "language_info": {"name": "python"}},
            "nbformat": 4, "nbformat_minor": 5}


def _split_cells(body: str) -> list[tuple[str, str]]:
    """`(title, source)` per `# %%` marker; index 0 is the untitled preamble."""
    cells = []
    pos, title = 0, ""
    for match in _CELL_MARK.finditer(body):
        cells.append((title, body[pos:match.start()]))
        title, pos = match.group(1).strip(), match.end() + 1
    cells.append((title, body[pos:]))
    return cells


def _set_parameter(source: str, name: str, value: str) -> str:
    """Rewrite `NAME = ...` in the parameters cell, keeping its comment."""
    pattern = re.compile(rf"^{re.escape(name)} = [^#\n]*?(\s*#[^\n]*)?$",
                         re.MULTILINE)
    new, count = pattern.subn(
        lambda m: f"{name} = {value!r}{m.group(1) or ''}", source, count=1)
    if count != 1:
        raise ValueError(
            f"{DIAGNOSE_SOURCE_NAME}: no `{name} = ...` line in the "
            f"parameters cell to fill in")
    return new


def build_diagnose_notebook(dataplane: pathlib.Path | None = None,
                            overrides: dict[str, object] | None = None
                            ) -> dict:
    """The environment diagnosis: header, parameters, inlined helpers, one
    cell per check, reading guide. Same inlining as the stages; no job.

    `overrides` is the same dict provision builds for the stages, so
    `CONFIG_PATH` becomes exactly the mount path the config was uploaded to.
    """
    root = dataplane or dataplane_dir()
    body = (root / DIAGNOSE_SOURCE_NAME).read_text(encoding="utf-8")
    body, needs_helpers = _strip_shared_import(body, DIAGNOSE_SOURCE_NAME)
    if not needs_helpers:
        raise ValueError(
            f"{DIAGNOSE_SOURCE_NAME}: expected an import of snowmig_source; "
            f"the connector check is meaningless without the helpers")
    module = ast.parse(body)
    header = ast.get_docstring(module) or ""
    if header and isinstance(module.body[0], ast.Expr):
        # The docstring is the notebook's markdown header, not a code cell.
        body = "".join(body.splitlines(keepends=True)[module.body[0].end_lineno:])
    sections = _split_cells(body)
    params = next((src for title, src in sections if title == "parameters"),
                  None)
    if params is None:
        raise ValueError(f"{DIAGNOSE_SOURCE_NAME}: no `# %% parameters` cell")
    for key, value in (overrides or {}).items():
        name = DIAGNOSE_PARAMS.get(key)
        if name and value is not None:
            params = _set_parameter(params, name, str(value))

    header += (f"\n\n---\n\n"
               f"*Generated from `engine/dataplane/{DIAGNOSE_SOURCE_NAME}` by "
               f"`engine/target/stage_notebooks.py`. Regenerate with "
               f"`snowmig.py build-notebooks`; do not hand-edit — an edit here "
               f"is overwritten on the next build. Change the source instead.*")
    cells = [
        _md(header), _code(params.strip("\n")),
        _md("## Shared source helpers\n\nInlined from "
            "`engine/dataplane/snowmig_source.py` so this notebook runs "
            "with nothing else uploaded beside it."),
        _code((root / SHARED_SOURCE_NAME).read_text(encoding="utf-8")),
    ]
    lead = sections[0][1].strip("\n")  # the imports, ahead of the first check
    for title, src in sections[1:]:
        if title == "parameters":
            continue
        if title.startswith("[markdown]"):
            heading = title[len("[markdown]"):].strip()
            text = "\n".join(line[2:] if line.startswith("# ") else line.lstrip("#")
                             for line in src.strip("\n").splitlines())
            cells.append(_md((f"## {heading}\n\n" if heading else "") + text))
            continue
        rule = "-" * max(3, 74 - len(title))
        code = f"# --- {title} {rule}\n{src.strip(chr(10))}"
        if lead:
            code, lead = f"{lead}\n\n{code}", ""
        cells.append(_code(code))
    return {"cells": cells,
            "metadata": {"snowmig": {"generated": True, "stage": "diagnose",
                                     "source": DIAGNOSE_SOURCE_NAME,
                                     "job": None},
                         "kernelspec": {"display_name": "Python 3",
                                        "language": "python",
                                        "name": "python3"},
                         "language_info": {"name": "python"}},
            "nbformat": 4, "nbformat_minor": 5}


def _write_notebook(path: pathlib.Path, nb: dict) -> pathlib.Path:
    # LF on every platform. `write_text` translates to CRLF on Windows, and
    # the committed notebooks are LF, so a rebuild there showed every line
    # changed.
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(nb, indent=1) + "\n")
    return path


def write_stage_notebooks(out_dir: str | pathlib.Path,
                          dataplane: pathlib.Path | None = None,
                          overrides: dict[str, object] | None = None
                          ) -> list[pathlib.Path]:
    """Write every stage notebook, plus the environment diagnosis, into
    `out_dir`. Returns the paths written."""
    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for stage in STAGES:
        nb = build_stage_notebook(stage, dataplane, overrides)
        written.append(_write_notebook(out / stage.notebook_name, nb))
    written.append(_write_notebook(
        out / DIAGNOSE_NOTEBOOK_NAME,
        build_diagnose_notebook(dataplane, overrides)))
    return written
