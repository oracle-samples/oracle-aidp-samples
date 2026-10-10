"""MCP server for fabric-aidp-migrator.

Exposes the five migration verbs as MCP tools so any MCP client (Codex,
Cursor, Claude Desktop) drives the same workflow the Claude Code plugin does.
Each tool shells out to the already-tested `fabric_aidp.cli` pipeline rather
than reimplementing logic, so the MCP surface cannot drift from the CLI.

The server, not the caller, decides where the CLI may read and write and
what it may see. An agent is steered by the files it inspects, so a tool
argument is not a trustworthy path:

* Every path argument resolves under one work root -- `FABRIC_AIDP_WORK_ROOT`,
  else the directory the server was started in. A `..` that climbs out, or an
  absolute path elsewhere, is refused before the CLI runs, with the resolved
  root in the message. The CLI runs with the root as its working directory,
  so the `.env` it reads is the root's. A default root that is a filesystem
  anchor or the home directory is refused: Claude Code starts the server in
  the project, but other clients start it in `/`, the home directory or
  their own install, and a root there contains nothing.
* An artifact named without a directory part (`inv.json`, `plan.json`,
  `migrated`) lives in a per-server-process run directory under the root, so
  two sessions that both accept the defaults cannot overwrite each other.
  Every tool result starts with the resolved root and paths, so the agent
  can see where things went.
* The CLI child gets an allowlisted environment (`ENV_ALLOWED_NAMES`,
  `ENV_ALLOWED_PREFIXES`), never the server's whole one. A developer shell
  carries GitHub, AWS, npm, PyPI and Slack credentials that have nothing to
  do with a migration; none of them reach the subprocess. The CLI's own
  settings pass by exact name, not by prefix, so an `AIDP_SECRET_*` or
  `FABRIC_PAT` exported for some other tool does not ride along either.
* `publish` has no `apply`. Writing to a live workspace is `fabric-aidp
  publish --apply`, typed by a person at the CLI.

Run:   fabric-aidp-mcp            (after `pip install -e '.[mcp]'`)
   or:  python3 -m fabric_aidp.mcp_server
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - exercised with the SDK stubbed
    # Almost nobody who reads this text typed the command. An MCP client ran
    # it, because the `.mcp.json` shipped in this repository names it, and a
    # plain `pip install -e .` does not install the SDK -- `mcp` needs Python
    # 3.10 while this tool supports 3.9, so it is an extra rather than a
    # requirement. The message arrives in a client's server log with no
    # surrounding context, so it has to say where it came from and what still
    # works without it.
    raise SystemExit(
        "fabric-aidp-mcp cannot start: the MCP SDK (1.x) is not installed.\n"
        "Install it with:  pip install -e '.[mcp]'   (from a clone of the repository)\n"
        f"           or:    {sys.executable} -m pip install 'mcp>=1.2,<2'   "
        "(any install, wheel included: this is the interpreter that ran it)\n"
        "It is an optional extra because `mcp` requires Python 3.10 and this "
        "tool supports 3.9.\n"
        "If you did not run this yourself, an MCP client started it from the "
        "`.mcp.json` in this repository. Nothing else needs the extra: the "
        "`fabric-aidp` CLI and the Claude Code slash commands work without it."
    ) from exc

mcp = FastMCP("fabric-aidp-migrator")

#: Names the directory every tool path must stay under. Read when a tool
#: runs rather than at import, so a client sets it per launch in `.mcp.json`
#: and nothing is baked in at the moment the module loads.
WORK_ROOT_VAR = "FABRIC_AIDP_WORK_ROOT"

#: Under the work root: where an artifact named without a directory part
#: goes. One directory per server process, named by UTC start time and pid.
RUNS_DIR = "fabric-aidp-runs"
RUN_ID = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + f"-{os.getpid()}"

#: Environment variables the CLI child receives, by exact name. Anything not
#: here or under `ENV_ALLOWED_PREFIXES` is dropped -- there is no deny-list to
#: keep up to date, so GITHUB_TOKEN, AWS_SECRET_ACCESS_KEY, NPM_TOKEN,
#: TWINE_PASSWORD, SLACK_TOKEN and the next one are out by construction.
ENV_ALLOWED_NAMES = frozenset({
    # Enough for an interpreter to start and find `node` and `aidp`.
    "PATH", "HOME", "LANG", "TMPDIR",
    # Windows. Without SYSTEMROOT a Python child does not start at all; the
    # rest are where Python, Node and the OCI SDK look for temp and config.
    "SYSTEMROOT", "COMSPEC", "PATHEXT", "TEMP", "TMP",
    "USERPROFILE", "APPDATA", "LOCALAPPDATA",
    # Python. `.mcp.json` sets PYTHONPATH so the child can import fabric_aidp
    # (and `_run` prepends this package's own root to it regardless).
    "PYTHONPATH", "PYTHONIOENCODING", "PYTHONUTF8", "PYTHONDONTWRITEBYTECODE",
    # OCI selectors -- which config file, profile, auth mode and region. They
    # name things; the secrets stay in the config file they point at.
    "OCI_CONFIG_FILE", "OCI_CONFIG_PROFILE", "OCI_CLI_PROFILE",
    "OCI_CLI_AUTH", "OCI_CLI_REGION",
    # `plan` reads this when neither flag nor manifest names a namespace.
    "OCI_NAMESPACE",
    # The two AIDP_ variables the CLI reads (`publish`, cli.py). By name, not
    # as the AIDP_ prefix: a shell that has run other AIDP tooling carries
    # AIDP_SECRET_*, AIDP_API_TOKEN and the like, none of which this CLI
    # looks at. Nothing reads a FABRIC_ variable; `.env` loading
    # (`_env.KEY_PREFIXES`) happens inside the child, from the root's file.
    "AIDP_WORKSPACE_KEY", "AIDP_CLUSTER_KEY",
})

#: The one prefix passed whole: locale. Every other variable is named above.
ENV_ALLOWED_PREFIXES = ("LC_",)

#: Windows extended-length prefixes. `Path.resolve()` keeps them, so a
#: `\\?\C:\root\x` and the unprefixed root `C:\root` share no common path
#: and the check would refuse a path that is inside the root.
_EXTENDED_UNC = "\\\\?\\UNC\\"
_EXTENDED = "\\\\?\\"


def _plain_path(value: str) -> str:
    r"""`value` without a Windows extended-length prefix (`\\?\`, `\\?\UNC\`)."""
    if value.startswith(_EXTENDED_UNC):
        return "\\\\" + value[len(_EXTENDED_UNC):]
    if value.startswith(_EXTENDED):
        return value[len(_EXTENDED):]
    return value


def _home() -> Optional[Path]:
    try:
        return Path.home().resolve()
    except (RuntimeError, OSError):  # no HOME / USERPROFILE to speak of
        return None


def work_root() -> Path:
    """The resolved work root: `$FABRIC_AIDP_WORK_ROOT`, else the server's cwd.

    The default is refused when it is a filesystem anchor (`/`, `C:\\`) or
    the home directory. Claude Code starts `.mcp.json` servers in the
    project, but Claude Desktop, Cursor and Codex start a stdio server in
    `/`, the home directory or their own install, and a root there would
    contain every file the user owns. An explicitly configured root is
    honoured as given: a dedicated drive is a legitimate choice.
    """
    configured = os.environ.get(WORK_ROOT_VAR)
    if configured:
        root = Path(_plain_path(configured) if os.name == "nt" else configured).resolve()
    else:
        root = Path.cwd().resolve()
        if root == Path(root.anchor) or root == _home():
            raise ValueError(
                f"the server started in {root}, which would make the whole "
                f"filesystem or home directory the work root. Set {WORK_ROOT_VAR} "
                f"to the migration directory in the client's server config.")
    if not root.is_dir():
        raise ValueError(f"{WORK_ROOT_VAR}={configured!r} is not a directory")
    return root


def _inside(root: Path, path: Path) -> bool:
    # normcase, because Windows compares paths case-insensitively and
    # resolve() may hand back the drive letter in either case.
    root_text, path_text = os.path.normcase(str(root)), os.path.normcase(str(path))
    try:
        return os.path.commonpath([root_text, path_text]) == root_text
    except ValueError:  # different drives on Windows: no common path at all
        return False


def resolve_path(value: str, *, label: str, artifact: bool = False) -> Path:
    """Resolve one tool path argument under the work root, or refuse it.

    A relative path resolves under the root; an absolute path must already
    be under it. `..` and symlinks are resolved before the check, so neither
    can climb out. With ``artifact=True`` a bare name -- no directory part --
    lands in this server's run directory, which is what lets the defaults
    `inv.json` -> `plan.json` -> `migrated` chain from one tool to the next
    without colliding with another session's. "Bare" is decided on the text
    as given: `./inv.json` and `inv.json/` name a directory, the root, and
    are honoured there rather than moved into the run directory.

    On Windows an extended-length prefix (`\\\\?\\`) is dropped first, so a
    tool that emits such paths can name a file inside the root.

    Raises ValueError naming the resolved root. Nothing has run yet: every
    tool resolves all of its paths before it calls `_run`.
    """
    text = str(value)
    if not text.strip():
        raise ValueError(f"{label}: an empty path is not a path")
    if os.name == "nt":
        text = _plain_path(text)
    root = work_root()
    given = Path(text)
    bare = (artifact and not given.is_absolute() and not given.drive
            and "/" not in text and os.sep not in text)
    if bare:
        candidate = root / RUNS_DIR / RUN_ID / given
    elif given.is_absolute():
        candidate = given
    else:
        candidate = root / given
    resolved = candidate.resolve()
    if not _inside(root, resolved):
        raise ValueError(
            f"{label} {value!r} resolves to {resolved}, outside the work root "
            f"{root} ({WORK_ROOT_VAR}). Use a path under the work root.")
    return resolved


def child_env(source=None) -> dict:
    """The environment the CLI child gets: `source` (default `os.environ`)
    filtered to `ENV_ALLOWED_NAMES` and `ENV_ALLOWED_PREFIXES`, nothing else."""
    source = os.environ if source is None else source
    env = {}
    for name, value in source.items():
        upper = name.upper()
        if upper in ENV_ALLOWED_NAMES or upper.startswith(ENV_ALLOWED_PREFIXES):
            env[name] = value
    return env


def _run(args, *, timeout: float = 300.0, resolved=()) -> str:
    """Run the CLI in the work root with the allowlisted environment.

    `resolved` is the `(label, Path)` list a tool built; it heads the result
    so the agent sees the root and the namespaced paths it actually used.
    """
    root = work_root()
    env = child_env()
    # The child must import the same `fabric_aidp` this server runs from,
    # however the server was launched (`pip install -e`, PYTHONPATH from
    # `.mcp.json`, or just started in a checkout). The checkout case used to
    # work only because the child inherited the server's cwd; it no longer
    # does, so put the package root on the child's path explicitly.
    package_root = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (package_root, env.get("PYTHONPATH", "")) if p)
    header = "\n".join([f"work root: {root}"]
                       + [f"{label}: {path}" for label, path in resolved]) + "\n\n"
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "fabric_aidp.cli", *args],
            cwd=str(root), env=env,
            # The CLI writes UTF-8 (cli._utf8_streams); decode it as such.
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=timeout,
            # Detach the child's stdin from the MCP stdio transport, or the
            # spawned CLI blocks at interpreter start on Windows.
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as exc:
        output = "".join(p for p in (exc.stdout, exc.stderr) if isinstance(p, str))
        return f"{header}[exit timeout after {timeout:g}s]\n{output}"
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        return f"{header}[exit {proc.returncode}]\n{out}"
    return header + (out or "[ok, no output]")


@mcp.tool()
def inventory(export_dir: Optional[str] = None, fixture: Optional[str] = None,
              sources: Optional[str] = None, tables_csv: Optional[str] = None,
              output: str = "inv.json") -> str:
    """Read-only scan of a Fabric Git export into a migration manifest.

    Paths resolve under the server's work root (`FABRIC_AIDP_WORK_ROOT`, else
    where the server started) and may not leave it. An `output` with no
    directory part goes to this session's run directory; the result's first
    lines say where.

    Args:
        export_dir: folder the Fabric workspace is Git-synced to.
        fixture: use the bundled demo estate instead ("demo").
        sources: comma-separated subset of notebook,warehouse,lakehouse,
            pipeline,semanticmodel,dataflow.
        tables_csv: optional CSV of known tables (catalog tier 3). Not the
            AIDP catalog -- that is a `plan` argument.
        output: manifest output path.
    """
    out = resolve_path(output, label="output", artifact=True)
    resolved = [("output", out)]
    args = ["inventory", "-o", str(out)]
    if fixture:
        args += ["--fixture", fixture]
    elif export_dir:
        export = resolve_path(export_dir, label="export_dir")
        resolved.append(("export_dir", export))
        args.append(str(export))
    if sources:
        args += ["--sources", sources]
    if tables_csv:
        tables = resolve_path(tables_csv, label="tables_csv")
        resolved.append(("tables_csv", tables))
        args += ["--tables-csv", str(tables)]
    return _run(args, resolved=resolved)


@mcp.tool()
def plan(manifest: str, output: str = "plan.json",
         namespace: Optional[str] = None, catalog: Optional[str] = None,
         lakehouses: Optional[str] = None) -> str:
    """Turn an inventory manifest into an AIDP mapping plan.

    Paths resolve under the server's work root and may not leave it. A
    `manifest` or `output` with no directory part is looked up in, or
    written to, this session's run directory -- so `inventory`'s default
    `inv.json` is found by name here.

    Args:
        manifest: path to the manifest produced by `inventory`.
        output: plan output path.
        namespace: OCI namespace for target buckets.
        catalog: AIDP catalog the table names sit in (default `default`).
            NOT the namespace.
        lakehouses: CSV of lakehouse GUID -> display name (`id`, `name`
            columns), which resolves the lakehouseId a Dataflow navigates by.
            Without it every such Dataflow read stays REVIEW (M10).
    """
    source = resolve_path(manifest, label="manifest", artifact=True)
    out = resolve_path(output, label="output", artifact=True)
    resolved = [("manifest", source), ("output", out)]
    args = ["plan", str(source), "-o", str(out)]
    if namespace:
        args += ["--namespace", namespace]
    if catalog:
        args += ["--catalog", catalog]
    if lakehouses:
        table = resolve_path(lakehouses, label="lakehouses")
        resolved.append(("lakehouses", table))
        args += ["--lakehouses", str(table)]
    return _run(args, resolved=resolved)


@mcp.tool()
def migrate(plan_path: str, filter: Optional[str] = None,
            out_dir: str = "migrated", catalog: Optional[str] = None) -> str:
    """Translate a plan (Fabric notebooks, Warehouse T-SQL, shortcut targets).

    Writes locally and contacts nothing. There is no mode to pass: this tool
    had a `demo` parameter that defaulted to True and appended a `--demo` the
    CLI ignored, so every call built a flag that did nothing.

    Paths resolve under the server's work root and may not leave it; a
    `plan_path` or `out_dir` with no directory part is this session's.

    Args:
        plan_path: path to the plan produced by `plan`.
        filter: only translate one slice, e.g. "notebook" or "warehouse".
        out_dir: output directory for artifacts and the report.
        catalog: re-target an existing plan to another AIDP catalog. Normally
            unset: the plan already carries the one `plan` recorded.
    """
    source = resolve_path(plan_path, label="plan_path", artifact=True)
    out = resolve_path(out_dir, label="out_dir", artifact=True)
    args = ["migrate", str(source), "-o", str(out)]
    if filter:
        args += ["--filter", filter]
    if catalog:
        args += ["--catalog", catalog]
    return _run(args, resolved=[("plan_path", source), ("out_dir", out)])


@mcp.tool()
def verify(report_or_dir: str = "migrated", filter: Optional[str] = None) -> str:
    """Classify a migration's output as PASS / REVIEW / SKIP / FAIL.

    PASS means no known issue was detected — not that the artifact runs.
    The path resolves under the server's work root and may not leave it; a
    bare name is this session's `migrate` output.

    Args:
        report_or_dir: path to report.json, or the directory containing it.
        filter: only verify one slice.
    """
    target = resolve_path(report_or_dir, label="report_or_dir", artifact=True)
    args = ["verify", str(target)]
    if filter:
        args += ["--filter", filter]
    return _run(args, resolved=[("report_or_dir", target)])


@mcp.tool()
def publish(out_dir: str = "migrated", prefix: str = "",
            cluster_key: Optional[str] = None) -> str:
    """Show what publishing a completed migration to AIDP would upload and create.

    Dry run only, and deliberately so: this tool has no `apply`. Publishing
    writes notebooks and creates jobs in a live workspace, which is not
    something an agent should be able to do without a person. Run
    `fabric-aidp publish <dir> --prefix <you> --apply` to actually publish.
    The path resolves under the server's work root and may not leave it.

    Args:
        out_dir: the directory `migrate` wrote.
        prefix: prefix for uploaded paths and job names.
        cluster_key: AIDP cluster key the jobs' tasks would run on.
    """
    target = resolve_path(out_dir, label="out_dir", artifact=True)
    args = ["publish", str(target)]
    if prefix:
        args += ["--prefix", prefix]
    if cluster_key:
        args += ["--cluster-key", cluster_key]
    return _run(args, resolved=[("out_dir", target)])


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
