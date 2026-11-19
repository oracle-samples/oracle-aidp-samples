"""MCP server for aws-aidp-migrator.

Exposes the four migration verbs as MCP tools so any MCP-compatible client
(OpenAI Codex, Cursor, Claude Desktop, etc.) gets the same workflow the Claude
Code plugin does. Each tool shells out to the already-tested `aws_aidp.cli`
pipeline rather than reimplementing logic.

Run:   aws-aidp-mcp                 (after `pip install -e '.[mcp]'`)
   or:  python3 -m aws_aidp.mcp_server
"""
from __future__ import annotations

import subprocess
import sys
from typing import Optional

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "The compatible MCP SDK (1.x) is required. Install it with: "
        "pip install -e '.[mcp]'"
    ) from e

mcp = FastMCP("aws-aidp-migrator")


def _run(args: list[str], *, timeout: float = 300.0) -> str:
    """Run `python -m aws_aidp.cli <args>` and return combined output."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "aws_aidp.cli", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            # Detach the child's stdin from the MCP stdio transport pipe, or the
            # spawned CLI blocks forever at interpreter start on Windows.
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as e:
        output = "".join(part for part in (e.stdout, e.stderr) if isinstance(part, str))
        return f"[exit timeout after {timeout:g}s]\n{output}"
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        return f"[exit {proc.returncode}]\n{out}"
    return out or "[ok, no output]"


@mcp.tool()
def inventory(
    region: str = "us-east-1",
    fixture: Optional[str] = None,
    sources: Optional[str] = None,
    output: str = "inv.json",
) -> str:
    """Read-only scan of an AWS data stack → migration manifest.

    Args:
        region: AWS region to scan (ignored if fixture is set).
        fixture: use a bundled fixture (e.g. "demo") instead of live AWS.
        sources: comma-separated subset of s3,glue,athena,emr,sagemaker.
        output: manifest output path.
    """
    args = ["inventory", "-o", output]
    if fixture:
        args += ["--fixture", fixture]
    else:
        args += ["--region", region]
    if sources:
        args += ["--sources", sources]
    return _run(args)


@mcp.tool()
def plan(manifest: str, output: str = "plan.json", namespace: Optional[str] = None) -> str:
    """Turn an inventory manifest into an AIDP mapping plan.

    Args:
        manifest: path to the inventory manifest (from `inventory`).
        output: plan output path.
        namespace: OCI namespace for target buckets.
    """
    args = ["plan", manifest, "-o", output]
    if namespace:
        args += ["--namespace", namespace]
    return _run(args)


@mcp.tool()
def migrate(
    plan_path: str,
    demo: bool = True,
    filter: Optional[str] = None,
    out_dir: str = "migrated",
) -> str:
    """Translate a migration plan (Athena→Spark SQL, Glue→PySpark).

    Args:
        plan_path: path to the plan (from `plan`).
        demo: offline mode — write artifacts locally, no live AIDP calls (default True).
        filter: only translate a slice (e.g. "athena" or "glue").
        out_dir: output directory for translated artifacts + report.
    """
    args = ["migrate", plan_path, "-o", out_dir]
    if demo:
        args.append("--demo")
    if filter:
        args += ["--filter", filter]
    return _run(args)


@mcp.tool()
def verify(report_or_dir: str = "migrated", filter: Optional[str] = None) -> str:
    """Classify a migration's output as PASS / REVIEW / SKIP / FAIL.

    Args:
        report_or_dir: path to a migrate report.json or the directory containing it.
        filter: only verify a slice (e.g. "athena").
    """
    args = ["verify", report_or_dir]
    if filter:
        args += ["--filter", filter]
    return _run(args)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
