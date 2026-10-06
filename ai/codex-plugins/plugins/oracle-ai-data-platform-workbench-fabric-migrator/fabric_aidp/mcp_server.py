"""MCP server for fabric-aidp-migrator.

Exposes the five migration verbs as MCP tools so any MCP client (Codex,
Cursor, Claude Desktop) drives the same workflow the Claude Code plugin does.
Each tool shells out to the already-tested `fabric_aidp.cli` pipeline rather
than reimplementing logic, so the MCP surface cannot drift from the CLI.

Run:   fabric-aidp-mcp            (after `pip install -e '.[mcp]'`)
   or:  python3 -m fabric_aidp.mcp_server
"""
from __future__ import annotations

import subprocess
import sys
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


def _run(args, *, timeout: float = 300.0) -> str:
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "fabric_aidp.cli", *args],
            # The CLI writes UTF-8 (cli._utf8_streams); decode it as such.
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=timeout,
            # Detach the child's stdin from the MCP stdio transport, or the
            # spawned CLI blocks at interpreter start on Windows.
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as exc:
        output = "".join(p for p in (exc.stdout, exc.stderr) if isinstance(p, str))
        return f"[exit timeout after {timeout:g}s]\n{output}"
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        return f"[exit {proc.returncode}]\n{out}"
    return out or "[ok, no output]"


@mcp.tool()
def inventory(export_dir: Optional[str] = None, fixture: Optional[str] = None,
              sources: Optional[str] = None, tables_csv: Optional[str] = None,
              output: str = "inv.json") -> str:
    """Read-only scan of a Fabric Git export into a migration manifest.

    Args:
        export_dir: folder the Fabric workspace is Git-synced to.
        fixture: use the bundled demo estate instead ("demo").
        sources: comma-separated subset of notebook,warehouse,lakehouse,
            pipeline,semanticmodel,dataflow.
        tables_csv: optional CSV of known tables (catalog tier 3). Not the
            AIDP catalog -- that is a `plan` argument.
        output: manifest output path.
    """
    args = ["inventory", "-o", output]
    if fixture:
        args += ["--fixture", fixture]
    elif export_dir:
        args.append(export_dir)
    if sources:
        args += ["--sources", sources]
    if tables_csv:
        args += ["--tables-csv", tables_csv]
    return _run(args)


@mcp.tool()
def plan(manifest: str, output: str = "plan.json",
         namespace: Optional[str] = None, catalog: Optional[str] = None,
         lakehouses: Optional[str] = None) -> str:
    """Turn an inventory manifest into an AIDP mapping plan.

    Relative paths resolve against this server's working directory, which is
    wherever the MCP client started it; pass absolute paths to be sure.

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
    args = ["plan", manifest, "-o", output]
    if namespace:
        args += ["--namespace", namespace]
    if catalog:
        args += ["--catalog", catalog]
    if lakehouses:
        args += ["--lakehouses", lakehouses]
    return _run(args)


@mcp.tool()
def migrate(plan_path: str, filter: Optional[str] = None,
            out_dir: str = "migrated", catalog: Optional[str] = None) -> str:
    """Translate a plan (Fabric notebooks, Warehouse T-SQL, shortcut targets).

    Writes locally and contacts nothing. There is no mode to pass: this tool
    had a `demo` parameter that defaulted to True and appended a `--demo` the
    CLI ignored, so every call built a flag that did nothing.

    Args:
        plan_path: path to the plan produced by `plan`.
        filter: only translate one slice, e.g. "notebook" or "warehouse".
        out_dir: output directory for artifacts and the report.
        catalog: re-target an existing plan to another AIDP catalog. Normally
            unset: the plan already carries the one `plan` recorded.
    """
    args = ["migrate", plan_path, "-o", out_dir]
    if filter:
        args += ["--filter", filter]
    if catalog:
        args += ["--catalog", catalog]
    return _run(args)


@mcp.tool()
def verify(report_or_dir: str = "migrated", filter: Optional[str] = None) -> str:
    """Classify a migration's output as PASS / REVIEW / SKIP / FAIL.

    PASS means no known issue was detected — not that the artifact runs.

    Args:
        report_or_dir: path to report.json, or the directory containing it.
        filter: only verify one slice.
    """
    args = ["verify", report_or_dir]
    if filter:
        args += ["--filter", filter]
    return _run(args)


@mcp.tool()
def publish(out_dir: str = "migrated", prefix: str = "",
            cluster_key: Optional[str] = None) -> str:
    """Show what publishing a completed migration to AIDP would upload and create.

    Dry run only, and deliberately so: this tool has no `apply`. Publishing
    writes notebooks and creates jobs in a live workspace, which is not
    something an agent should be able to do without a person. Run
    `fabric-aidp publish <dir> --prefix <you> --apply` to actually publish.

    Args:
        out_dir: the directory `migrate` wrote.
        prefix: prefix for uploaded paths and job names.
        cluster_key: AIDP cluster key the jobs' tasks would run on.
    """
    args = ["publish", out_dir]
    if prefix:
        args += ["--prefix", prefix]
    if cluster_key:
        args += ["--cluster-key", cluster_key]
    return _run(args)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
