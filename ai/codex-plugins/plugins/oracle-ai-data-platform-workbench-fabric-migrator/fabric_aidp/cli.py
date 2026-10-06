"""`fabric-aidp <verb>` — inventory, plan, migrate, verify."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from fabric_aidp import __version__
from fabric_aidp._env import load_dotenv
from fabric_aidp.inventory.manifest import ALL_SOURCES
from fabric_aidp.namespace import PLACEHOLDER
from fabric_aidp.naming import DEFAULT_CATALOG

_DEFAULT_NAMESPACE = PLACEHOLDER


def _load_json_object(path: Path, label: str) -> dict:
    value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object: {path}")
    return value


def _parse_sources(raw):
    if raw is None:
        return ALL_SOURCES
    requested = tuple(part.strip().lower() for part in raw.split(",") if part.strip())
    sources = tuple(dict.fromkeys(requested))
    bad = [s for s in sources if s not in ALL_SOURCES]
    if not sources or bad:
        detail = f"unknown source(s): {bad}" if bad else "source list is empty"
        raise ValueError(f"{detail}; valid: {ALL_SOURCES}")
    return sources


def _namespace(args, manifest=None) -> str:
    """The OCI namespace for this run, most specific source first.

    A flag someone typed beats ambient configuration even when they typed it
    one verb earlier, so the namespace `inventory --namespace` recorded in
    this manifest outranks $OCI_NAMESPACE. `.env` feeds that variable; it is
    loaded in main(), for every verb, because loading it only in `inventory`
    is how `plan` came to ignore an OCI_NAMESPACE the user had set.
    """
    recorded = (manifest or {}).get("oci_namespace")
    return (args.namespace or recorded or os.environ.get("OCI_NAMESPACE")
            or _DEFAULT_NAMESPACE)


def cmd_inventory(args) -> int:
    from fabric_aidp.inventory.manifest import build_manifest, summarize, write_manifest

    if args.fixture:
        from fabric_aidp.fixtures import demo_workspace_path
        export_dir = demo_workspace_path()
        print(f"[fixture] using {export_dir}")
    elif args.export_dir:
        export_dir = Path(args.export_dir)
    else:
        raise ValueError("give an export directory, or --fixture demo")

    sources = _parse_sources(args.sources)
    log = (lambda line: print(f"[{time.strftime('%H:%M:%S')}] {line}", flush=True)) \
        if args.verbose else None
    manifest = build_manifest(export_dir, sources,
                              catalog_csv=args.tables_csv,
                              oci_namespace=args.namespace, log=log)
    out = Path(args.output) if args.output else Path(
        f"inventory-{manifest['workspace_name']}-{time.strftime('%Y%m%dT%H%M%S')}.json")
    write_manifest(manifest, out)
    print(f"\n# wrote {out}\n")
    print(summarize(manifest))
    return 0


def cmd_plan(args) -> int:
    from fabric_aidp.plan.planner import (build_plan, load_supplied_lakehouses,
                                          summarize_plan, write_plan)

    manifest = _load_json_object(Path(args.manifest), "manifest")
    lakehouses = (load_supplied_lakehouses(args.lakehouses)
                  if args.lakehouses else None)
    plan = build_plan(manifest, oci_namespace=_namespace(args, manifest),
                      catalog=args.catalog, lakehouses=lakehouses)
    out = Path(args.output) if args.output else Path(args.manifest).with_suffix(
        ".plan.json")
    write_plan(plan, out)
    print(f"# wrote {out}\n")
    print(summarize_plan(plan))
    return 0


def cmd_migrate(args) -> int:
    from fabric_aidp.migrate.runner import migrate

    plan = _load_json_object(Path(args.plan), "plan")
    out_dir = Path(args.out_dir or "./migrated")
    print(f"# plan={plan.get('plan_id')}  mode=offline  "
          f"filter={args.filter or 'all'}\n")
    report = migrate(plan, out_dir=out_dir, filter_kind=args.filter,
                     catalog=args.catalog, log=lambda line: print(line, flush=True))
    counts = report["counts"]
    print(f"\n# done. ok={counts.get('ok', 0)}  "
          f"needs_review={counts.get('needs_manual_review', 0)}  "
          f"blocked={counts.get('blocked', 0)}  "
          f"planned={counts.get('planned', 0)}  error={counts.get('error', 0)}")
    print(f"# wrote {out_dir}/report.json, report.md and report.html")
    return 0 if counts.get("error", 0) == 0 else 1


def cmd_publish(args) -> int:
    from fabric_aidp.publish import PublishError, publish

    workspace = args.workspace_key or os.environ.get("AIDP_WORKSPACE_KEY")
    cluster = args.cluster_key or os.environ.get("AIDP_CLUSTER_KEY")
    if args.apply and not workspace:
        print("error: --workspace-key (or AIDP_WORKSPACE_KEY) is required "
              "with --apply", file=sys.stderr)
        return 2
    try:
        result = publish(
            args.out_dir, workspace_key=workspace, cluster_key=cluster,
            prefix=args.prefix, instance_id=args.instance_id,
            profile=args.profile, auth=args.auth, apply=args.apply,
            reuse_existing=args.reuse_existing_notebooks,
            log=lambda line: print(line))
    except PublishError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    refused_notebooks = result.get("refused_notebooks") or []
    for entry in refused_notebooks:
        print(f"  REFUSED  notebook {entry['notebook']}: {entry['reason']}")
    for entry in result["blocked"]:
        print(f"  REFUSED  {entry['job']}: {entry['reason']}")
    refused = len(result["blocked"]) + len(refused_notebooks)
    if not args.apply:
        print(f"\nwould upload {len(result['notebooks'])} notebook(s) and create "
              f"{len(result['jobs'])} job(s); {refused} refused.")
        for upload in result["notebooks"]:
            print(f"    {upload['remote']}")
        for job in result["jobs"]:
            print(f"    job {job['name']}  ({len(job['definition']['tasks'])} tasks)")
        print("\nnothing was sent. add --apply to publish.")
        return 0

    # A refused job -- at plan time, or because its notebook was not uploaded
    # by this run -- is a job the user asked for and did not get: exit 1, so
    # CI does not go green. "Already exists" is an idempotent re-run: exit 0.
    # A refused notebook is the same: it was asked for and not sent.
    failed = ([x for x in result["notebooks"] + result["jobs"]
               if x.get("status") in ("error", "refused")]
              + list(result["blocked"]) + list(refused_notebooks))
    skipped = sum(1 for x in result["notebooks"] + result["jobs"]
                  if x.get("status") == "skipped")
    wanted = len(result["jobs"]) + len(result["blocked"])
    print(f"\nuploaded {sum(1 for n in result['notebooks'] if n['status'] == 'uploaded')}"
          f"/{len(result['notebooks']) + len(refused_notebooks)} notebook(s); "
          f"created {sum(1 for j in result['jobs'] if j['status'] == 'created')}"
          f"/{wanted} job(s); {skipped} skipped, {len(failed)} failed or refused.")
    return 1 if failed else 0


def cmd_verify(args) -> int:
    from fabric_aidp.verify.checker import format_verify, verify

    report_path = Path(args.report_or_dir)
    if report_path.is_dir():
        report_path = report_path / "report.json"
    if not report_path.exists():
        print(f"error: {report_path} not found", file=sys.stderr)
        return 2
    result = verify(report_path, filter_kind=args.filter)
    print(format_verify(result))
    return 0 if result["summary"]["FAIL"] == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fabric-aidp",
        description="Microsoft Fabric → Oracle AIDP migrator")
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="cmd", required=True)

    inventory = sub.add_parser(
        "inventory", help="scan a Fabric Git export (read-only) and emit a manifest")
    inventory.add_argument("export_dir", nargs="?",
                           help="folder your Fabric workspace is Git-synced to")
    inventory.add_argument("--fixture", choices=("demo",),
                           help="use the bundled demo estate instead of a real export")
    inventory.add_argument("--sources",
                           help=f"comma-separated subset of {','.join(ALL_SOURCES)}")
    # Named `--catalog` until this release, which put two unrelated meanings
    # on one flag name: here a CSV of known tables, on `plan`/`migrate` the
    # AIDP catalog a table name is built under. Nothing is released, so it
    # is renamed rather than aliased.
    inventory.add_argument("--tables-csv",
                           help="optional CSV of known tables (tier 3 of catalog "
                                "resolution); requires a 'table' column. NOT the "
                                "AIDP catalog -- that is `plan --catalog`")
    inventory.add_argument("--namespace",
                           help="OCI namespace; recorded in the manifest and "
                                "used by `plan` unless `plan --namespace` "
                                "overrides it")
    inventory.add_argument("-v", "--verbose", action="store_true")
    inventory.add_argument("-o", "--output", help="manifest output path")
    inventory.set_defaults(func=cmd_inventory)

    plan = sub.add_parser("plan", help="produce a migration plan from a manifest")
    plan.add_argument("manifest")
    plan.add_argument("-o", "--output")
    plan.add_argument("--namespace",
                      help="OCI namespace for target buckets. Default: the "
                           "namespace recorded by `inventory --namespace`, "
                           "else $OCI_NAMESPACE (a .env is read)")
    plan.add_argument("--catalog", default=DEFAULT_CATALOG,
                      help="AIDP catalog that table names sit in. NOT the same "
                           "as --namespace, which is the OCI object-storage "
                           "namespace used in oci:// paths")
    # A Dataflow navigates by `lakehouseId`, a workspace item id, and a Git
    # export writes that down in exactly one place -- a notebook bound to
    # the lakehouse. With no such notebook the name is unrecoverable and
    # the read is flagged for a human even when the table resolved. This is
    # the operator supplying what the export cannot say, exactly as
    # `inventory --tables-csv` does for tables.
    plan.add_argument("--lakehouses",
                      help="optional CSV of lakehouse GUID -> display name; "
                           "requires 'id' and 'name' columns. Resolves the "
                           "lakehouseId a Dataflow navigates by when no "
                           "notebook in the export is bound to it. NOT the "
                           "AIDP catalog (--catalog) and not a table list "
                           "(`inventory --tables-csv`)")
    plan.set_defaults(func=cmd_plan)

    # `--demo` was here, accepted and ignored. Removed rather than kept
    # forever, for the same reason `inventory --catalog` above was renamed
    # rather than aliased: nothing is released (CHANGELOG: "0.1.0 --
    # unreleased ... Nothing has been published to an index yet"), so the
    # only callers passing it were in this repository. It costs any script
    # that still does an exit 2 -- "unrecognized arguments: --demo" -- and
    # 0.1.0 is the last moment that cost is this small.
    migrate = sub.add_parser(
        "migrate", help="translate a plan into artifacts, written locally")
    migrate.add_argument("plan")
    migrate.add_argument("-o", "--out-dir", help="output directory (default: ./migrated)")
    migrate.add_argument("--filter", choices=sorted(ALL_SOURCES),
                         help="only run one source slice")
    migrate.add_argument("--catalog", default=None,
                         help="AIDP catalog that table names sit in. Normally "
                              "left unset: the plan already carries the catalog "
                              "recorded by `plan --catalog`, and that is what is "
                              "used. Set this only to re-target an existing plan "
                              "to a different catalog without re-planning. NOT "
                              "the same as --namespace, which is the OCI "
                              "object-storage namespace used in oci:// paths")
    migrate.set_defaults(func=cmd_migrate)

    verify = sub.add_parser(
        "verify", help="classify a migrate report into PASS / REVIEW / SKIP / FAIL")
    verify.add_argument("report_or_dir",
                        help="path to report.json, or the directory containing it")
    verify.add_argument("--filter", choices=sorted(ALL_SOURCES))
    verify.set_defaults(func=cmd_verify)

    published = sub.add_parser(
        "publish", help="upload a completed migration into an AIDP workspace")
    published.add_argument("out_dir", help="the directory `migrate` wrote")
    published.add_argument("--apply", action="store_true",
                           help="actually publish; without it this is a dry run")
    published.add_argument("--workspace-key", default=None,
                           help="AIDP workspace key (or $AIDP_WORKSPACE_KEY)")
    published.add_argument("--cluster-key", default=None,
                           help="AIDP cluster key for every task "
                                "(or $AIDP_CLUSTER_KEY)")
    published.add_argument("--prefix", default="",
                           help="prefix for uploaded paths and job names, so two "
                                "people publishing into one workspace do not "
                                "collide; required with --apply")
    published.add_argument("--instance-id", default=None,
                           help="AIDP instance OCID (or $AIDP_INSTANCE_ID)")
    published.add_argument("--profile", default=None, help="OCI config profile")
    published.add_argument("--reuse-existing-notebooks", action="store_true",
                           help="let jobs run notebooks already at their target paths "
                                "(finishing a run whose job creation failed); they are "
                                "still never overwritten")
    published.add_argument("--auth", default=None,
                           help="OCI auth mode, e.g. api_key or security_token")
    published.set_defaults(func=cmd_publish)
    return parser


def _utf8_streams() -> None:
    """Fabric names are Unicode; a redirected stdout on Windows is cp1252.

    Without this, `--help | more` crashed on the arrow in the description and
    `migrate` died on its first Arabic or CJK item name, leaving no report.
    """
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def main(argv=None) -> int:
    _utf8_streams()
    # Every verb, not just `inventory`. `publish` reads AIDP_WORKSPACE_KEY
    # and `plan` reads OCI_NAMESPACE out of the environment, and neither saw
    # a .env because only `inventory` loaded one.
    load_dotenv()
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError,
            ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
