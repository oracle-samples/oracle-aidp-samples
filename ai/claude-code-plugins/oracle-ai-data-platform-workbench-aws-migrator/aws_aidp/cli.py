"""`aws-aidp <verb>` — 4 verbs: inventory, plan, migrate, verify."""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from importlib.resources import files
from pathlib import Path

from aws_aidp import __version__
from aws_aidp._env import load_dotenv


SOURCE_NAMES = ("s3", "glue", "athena", "emr", "sagemaker")
_FIXTURE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


def _load_json_object(path: Path, label: str) -> dict:
    """Load a command input and fail closed when its JSON root is not an object."""
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object: {path}")
    return value


def _fixture_path(name: str):
    """Resolve a bundled fixture name without allowing filesystem traversal."""
    if not isinstance(name, str) or not _FIXTURE_NAME_RE.fullmatch(name):
        raise ValueError(
            "invalid fixture name; use 1-64 letters, digits, underscores, or hyphens"
        )
    resource = files("aws_aidp.fixtures").joinpath(f"{name}-manifest.json")
    if not resource.is_file():
        raise ValueError(f"bundled fixture not found: {name}")
    return resource


def _parse_sources(raw: str | None) -> tuple[str, ...]:
    if raw is None:
        return SOURCE_NAMES
    requested = tuple(part.strip().lower() for part in raw.split(",") if part.strip())
    # Preserve user order, but never scan the same API twice.
    sources = tuple(dict.fromkeys(requested))
    bad = [source for source in sources if source not in SOURCE_NAMES]
    if not sources or bad:
        detail = f"unknown source(s): {bad}" if bad else "source list is empty"
        raise ValueError(f"{detail}; valid: {SOURCE_NAMES}")
    return sources


def cmd_inventory(args: argparse.Namespace) -> int:
    load_dotenv()
    from aws_aidp.inventory.manifest import summarize, write_manifest

    sources = _parse_sources(args.sources)
    if args.fixture is not None:
        fixture_path = _fixture_path(args.fixture)
        manifest = _load_json_object(fixture_path, "fixture manifest")
        fixture_sources = manifest.get("sources")
        if not isinstance(fixture_sources, dict):
            raise ValueError("fixture manifest field 'sources' must be a JSON object")
        missing = [source for source in sources if source not in fixture_sources]
        if missing:
            raise ValueError(
                "fixture does not contain requested source(s): " + ", ".join(missing)
            )
        for source in sources:
            source_data = fixture_sources[source]
            if not isinstance(source_data, dict):
                raise ValueError(f"fixture source {source!r} must be a JSON object")
            if not isinstance(source_data.get("summary", {}), dict):
                raise ValueError(f"fixture source {source!r}.summary must be a JSON object")
        manifest["sources"] = {source: fixture_sources[source] for source in sources}
        manifest["sources_scanned"] = list(sources)
        print(f"[fixture] using {fixture_path}")
    else:
        from aws_aidp.aws_client import AwsClient, AwsConfig, AwsAuthError
        from aws_aidp.inventory.manifest import build_manifest
        cfg = AwsConfig.from_env(region=args.region)
        client = AwsClient(cfg)
        log = lambda line: print(f"[{time.strftime('%H:%M:%S')}] {line}", flush=True)
        try:
            manifest = build_manifest(client, sources, log=log)
        except AwsAuthError as e:
            print(f"error: {e}", file=sys.stderr)
            return 2

    region = manifest.get("region", "unknown")
    out = Path(args.output) if args.output else Path(f"inventory-{region}-{time.strftime('%Y%m%dT%H%M%S')}.json")
    write_manifest(manifest, out)
    print(f"\n# wrote {out}\n")
    print(summarize(manifest))
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    import os
    from aws_aidp.plan import build_plan, write_plan, summarize_plan

    manifest = _load_json_object(Path(args.manifest), "manifest")
    ns = args.namespace
    if ns is None:
        ns = os.environ.get("OCI_NAMESPACE") or "<your-oci-namespace>"
    plan = build_plan(manifest, oci_namespace=ns)
    out = Path(args.output) if args.output else Path(args.manifest).with_suffix(".plan.json")
    write_plan(plan, out)
    print(f"# wrote {out}\n")
    print(summarize_plan(plan))
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    from aws_aidp.migrate import migrate

    plan = _load_json_object(Path(args.plan), "plan")
    out_dir = Path(args.out_dir or "./migrated")
    log = lambda line: print(line, flush=True)
    print(f"# plan={plan.get('plan_id')}  mode={'demo' if args.demo else 'live'}  filter={args.filter or 'all'}\n")
    report = migrate(plan, out_dir=out_dir, filter_kind=args.filter, demo=args.demo, log=log)
    c = report["counts"]
    print(f"\n# done. ok={c.get('ok', 0)}  needs_review={c.get('needs_manual_review', 0)}  "
          f"planned={c.get('planned', 0)}  error={c.get('error', 0)}")
    print(f"# wrote {out_dir}/report.json and {out_dir}/report.md")
    return 0 if c.get("error", 0) == 0 else 1


def cmd_verify(args: argparse.Namespace) -> int:
    from aws_aidp.verify import verify, format_verify

    report_path = Path(args.report_or_dir)
    if report_path.is_dir():
        report_path = report_path / "report.json"
    if not report_path.exists():
        print(f"error: {report_path} not found", file=sys.stderr)
        return 2
    result = verify(report_path, filter_kind=args.filter)
    print(format_verify(result))
    s = result["summary"]
    return 0 if s["FAIL"] == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="aws-aidp", description="AWS data stack → Oracle AIDP migrator")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    inv = sub.add_parser("inventory", help="scan AWS sources (read-only) and emit a manifest")
    inv.add_argument("--region", help="AWS region (default: $AWS_REGION or us-east-1)")
    inv.add_argument("--sources", help="comma-separated subset of s3,glue,athena,emr,sagemaker (default: all)")
    inv.add_argument("--fixture", help="load a pre-built fixture manifest instead of scanning AWS (e.g. 'demo')")
    inv.add_argument("-o", "--output", help="manifest output path (default: ./inventory-<region>-<ts>.json)")
    inv.set_defaults(func=cmd_inventory)

    pl = sub.add_parser("plan", help="produce a migration plan from a manifest")
    pl.add_argument("manifest")
    pl.add_argument("-o", "--output")
    pl.add_argument("--namespace", help="OCI namespace for target buckets (default: $OCI_NAMESPACE)")
    pl.set_defaults(func=cmd_plan)

    mg = sub.add_parser("migrate", help="execute a migration plan; --demo writes artifacts locally")
    mg.add_argument("plan")
    mg.add_argument("-o", "--out-dir", help="output directory for translated artifacts (default: ./migrated)")
    mg.add_argument(
        "--filter", choices=SOURCE_NAMES,
        help="only run one source slice",
    )
    mg.add_argument("--demo", action="store_true", help="offline mode: write artifacts + report, no AIDP calls")
    mg.set_defaults(func=cmd_migrate)

    vf = sub.add_parser("verify", help="classify a migrate report into PASS / REVIEW / SKIP / FAIL")
    vf.add_argument("report_or_dir", help="path to migrate report.json (or the directory containing it)")
    vf.add_argument("--filter", choices=SOURCE_NAMES, help="only verify one source slice")
    vf.set_defaults(func=cmd_verify)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
