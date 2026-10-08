#!/usr/bin/env python3
"""snowmig -- Snowflake -> AIDP migrator CLI.

One subcommand per pipeline stage (`--help` lists them all). Each reads the
previous stage's JSON and writes its own plus a markdown report, so any stage
can be re-run alone. The main ones:

  assess  -> inventory.json      + INVENTORY.md            (needs Snowflake)
  deps    -> dependencies.json                              (needs Snowflake)
  plan    -> plan.json           + PLANNED_OBJECTS.md       (offline)
  ddl     -> ddl_plan.json       + DDL_PLAN.md              (offline)
  catalog -> catalog_result.json + CATALOG.md               (dry-run offline;
                                                             --execute needs AIDP)
  deploy  -> deploy_result.json  + SOFT_CLONE_SUMMARY.md    (dry-run offline;
                                                             --execute needs AIDP)
  compute -> compute.json        + COMPUTE_PROPOSAL.md      (needs Snowflake)
  smoke   -> smoke.json          + SMOKE_TEST.md            (source; dest if given)
  notebook-> <nb>.ipynb          + NOTEBOOK.md              (offline; --upload is
                                                             a dry run, refused
                                                             with --execute)
  summary -> SUMMARY.md                                     (offline)
  data-options -> data_options.json + DATA_MOVEMENT_OPTIONS.md  (offline; the
                  options are PROPOSALS. This CLI copies no rows itself; the
                  one implemented copy is the in-AIDP snowmig_02_copy_schema
                  job, run schema by schema by the operator)
  demo    -> every artifact above + DEMO.md                 (offline; DEV MODE --
             the whole pipeline against an EMULATED estate and an EMULATED AIDP,
             so the flow can be understood with no credentials and no risk)

Bronze mirrors the source: Snowflake database -> AIDP catalog, schema -> schema,
table -> table, view -> view. Silver and Gold get disabled job stubs.

`catalog` registers an EXTERNAL/SNOWFLAKE catalog by default -- a read-only
pointer at the live source that copies nothing. The STANDARD target catalog
(`catalog --catalog-type standard`, runbook S4) is created as a container; its
schemas and tables are created on AIDP compute by the structure job
(`run --job snowmig_01_structure`, runbook S10), not through the catalog CRUD
API. Rows move only when the operator runs `snowmig_02_copy_schema`.

Exit codes: 0 ok | 1 error | 3 HALT: a condition to resolve with the user --
an identifier-case or target-name collision (assess, plan), or a column type
the target refuses at CREATE TABLE (ddl; usually TIMESTAMP_NTZ, remedy
`ddl --timestamp-ntz timestamp`)
"""
from __future__ import annotations

import argparse
import copy
import dataclasses
import datetime
import json
import os
import pathlib
import re
import sys
import tempfile

from plan.build import SECURE_VIEW_MODES, TargetCollision, build_plan
from plan.medallion import SCHEMA_STYLES
from plan.restrictions import InvalidRestriction
from plan.data_movement import OPTIONS as DATA_OPTIONS
from plan.data_movement import options_for, record_choice
from plan.smoke import run_smoke, smoke_verdict
from target.notebook import build_notebook, notebook_workspace_path
import retry
from report.diagram import phase_diagram
from report.stages import build_stage_board, phase_report, stage_for
from report.render import (
    DATA_OPTIONS_NOTE,
    render_catalog, render_catalogs, render_databases,
    render_census, render_maintenance, render_preflight,
    render_phase_report, render_stages,
    render_security,
    render_compute, render_ddl_plan, render_inventory, render_planned_objects,
    render_data_options, render_smoke, render_soft_clone_summary,
    render_summary, render_translation_map,
)
from report.translation_map import build_translation_map
from report.tokens import (build_token_report, record_stage_run,
                           render_tokens, tokens_section)
from snowflake_source.conn import (
    AuthError, SourceWriteRefused, build_connect_kwargs, connect,
    drop_secondary_roles, make_run_sql,
)
from snowflake_source.dialect import lexer
from snowflake_source.extract.catalog import (
    ROW_COUNT_MODES, build_inventory)
from snowflake_source.extract.maintenance import build_maintenance
from snowflake_source.extract.census import (build_census,
                                             secondary_roles_active)
from snowflake_source.extract.security import build_security
from snowflake_source.dialect.types import (
    GEOSPATIAL_MODES, SEMI_STRUCTURED_MODES, TIMESTAMP_NTZ_MODES, map_type)
from snowflake_source.extract.dependencies import extract_dependencies
from snowflake_source.extract.warehouses import extract_warehouses
from sizing.warehouse_map import propose_all
from target.coords import MissingTarget, resolve_target
from target.ddl import build_ddl_payload
from target.catalog_deploy import RefusedToExecute as DeployRefused
from target.catalog_deploy import deploy_catalog
from target.catalog_api import normalize_catalog_type
from target.stage_notebooks import STAGES, dataplane_dir
from target.catalog_provision import ensure_catalog
from target.catalog_provision import RefusedToExecute as CatalogRefused
from target.snowflake_catalog_connection import (
    ConnectionConfigError, build_snowflake_connection_details,
)
from migration_config import (
    CONFIG_NAMES, TEMPLATE_NAME, ConfigError, aidp_block, discover_config,
    compute_block, decisions_block, load_config, mapping_block, redact,
    reporting_block, resolve_secret, retry_block, teardown_block,
    snowflake_block,
    write_template,
)
from target.deploy import RefusedToExecute, deploy
from target.jobs import (COLD_START_RESTARTS, COLD_START_SECONDS,
                         JobRunCollision)
from target.provisioning import ProvisionTransportError
from target.executor import (
    NoBackendAvailable, detect_backend,
)
# Aliased: snowflake_source.conn also exports make_run_sql, and the
# unqualified import shadowed it.
# Two distinct BackendError classes exist (runner's for CLI exits, executor's
# for HTTP errors carried in the body); both must be caught or a live 400
# prints as a traceback instead of a message -- observed live.
from target.executor import BackendError as ExecutorBackendError
from target.runner import DEFAULT_CLI_TIMEOUT, BackendError
from target.runner import CatalogTransportError, make_call
from target.runner import make_run_sql as make_aidp_run_sql

HALT = 3


def _read(out_dir: pathlib.Path, name: str) -> dict:
    path = out_dir / name
    if not path.is_file():
        raise FileNotFoundError(
            f"{name} not found in {out_dir}. Run the earlier stage first.")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        # Name the file: the decoder's own message says where in the text
        # the problem is, not which artifact holds it.
        raise ValueError(
            f"{path} is not valid JSON ({exc}); delete it and re-run the "
            f"stage that produces it") from exc


def _write(out_dir: pathlib.Path, name: str, payload) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        (out_dir / name).write_text(payload, encoding="utf-8")
    else:
        (out_dir / name).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"  -> {out_dir / name}")


def _executed_record_exists(out_dir: pathlib.Path, name: str) -> bool:
    """Does `name` in `out_dir` record an EXECUTED run (`dry_run: false`)?

    A missing, unreadable or hand-edited file is "no executed record": the
    guard exists to protect evidence, and a file that is not evidence is
    not protected by it.
    """
    path = out_dir / name
    if not path.is_file():
        return False
    try:
        prev = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(prev, dict) and prev.get("dry_run") is False


def _refuse_dry_run_overwrite(out_dir: pathlib.Path, name: str,
                              stage: str) -> int:
    """The write stages default to a dry run, and a dry run writes the same
    artifact an executed run does. Re-running `deploy` to re-read
    PREFLIGHT.md, or forgetting --execute, therefore replaced the only local
    record of what was created, verified and burned with a dry-run record --
    and the stage board then said nothing had been created. The artifact is
    the evidence; it is not overwritten by a rehearsal."""
    print(f"error: {out_dir / name} records an EXECUTED {stage} run "
          f"(dry_run: false). A dry run would overwrite the only local "
          f"evidence of what was created and verified, so it was not written. "
          f"Re-run with --execute to apply it to the same target, or pass a "
          f"different --out-dir for a rehearsal.", file=sys.stderr)
    return 1


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _config_path(args, *, required: bool = False) -> pathlib.Path | None:
    """The config file to use: the flag, else the working directory, else the
    plugin's own directory. Returns None when there is none and one is not
    required, so the flag-only way of driving the CLI still works."""
    try:
        return discover_config(getattr(args, "config", None),
                               plugin_root=PLUGIN_ROOT)
    except ConfigError:
        if required or getattr(args, "config", None):
            raise
        return None


def _load_migration_config(args) -> dict:
    """The migration config, or {} when there is none. Read once per run."""
    cached = getattr(args, "_migration_config", None)
    if cached is not None:
        return cached
    path = _config_path(args)
    config = load_config(path) if path else {}
    if path and not getattr(args, "_config_announced", False):
        # Say which file the run is using: with discovery, "the config" is no
        # longer necessarily the one the operator had in mind.
        print(f"  config: {path}")
        args._config_announced = True
    args._migration_config = config
    return config


def _aidp_from_config(args) -> dict:
    """AIDP coordinates the config supplies, announced rather than assumed.

    A destination read from a file is exactly the thing that used to be
    forbidden here, so it is printed: the operator sees which environment
    the next command is aimed at, and writing still needs `--execute`.
    """
    block = aidp_block(_load_migration_config(args))
    if not block:
        return {}
    taken = {k: v for k, v in block.items()
             if not getattr(args, k.replace("-", "_"), None)}
    if taken:
        # `oci_profile` has its own line (see _oci_profile). `subnet_id` is
        # accepted so a typo is still reported, but no stage reads it from
        # the file yet -- said here rather than left to be assumed.
        shown = ", ".join(
            f"{k}={v}" + (" (not used by any stage yet)"
                          if k == "subnet_id" else "")
            for k, v in sorted(taken.items()) if k != "oci_profile")
        if shown:
            print(f"  destination from the config file: {shown}")
    return block


def _oci_profile(args) -> str | None:
    """`aidp.oci_profile` from the config, announced once.

    `_oci_runner` passes it as `--profile` to every `oci` and `aidp` call.
    Without it, each CLI resolves its own profile (`OCI_CLI_PROFILE`, else
    DEFAULT).
    """
    cached = getattr(args, "_oci_profile", None)
    if cached is not None:
        return cached or None
    profile = str(aidp_block(_load_migration_config(args))
                  .get("oci_profile") or "").strip()
    args._oci_profile = profile  # "" when absent, so this runs once
    if profile:
        print(f"  oci profile from the config file: {profile} (passed to "
              f"every `oci` and `aidp` call)")
    return profile or None


# A child CLI is another Python program with its own interpreter and its own
# site-packages. These variables retarget that interpreter, and a virtual
# environment built from Microsoft Store Python exports PYTHONUSERBASE
# unconditionally -- which made `oci` die with "No module named
# 'cryptography'", reported as a failed API call rather than a broken
# environment. Nothing the plugin needs travels in them, so they are dropped.
_INHERITED_PYTHON_VARS = (
    "PYTHONHOME", "PYTHONPATH", "PYTHONUSERBASE", "PYTHONNOUSERSITE",
    "PYTHONSTARTUP", "PYTHONEXECUTABLE", "PYTHONSAFEPATH",
)


def cli_environment(environ: dict | None = None) -> dict:
    """The environment a spawned CLI should see: ours, minus the variables
    that would repoint its interpreter."""
    env = dict(os.environ if environ is None else environ)
    for name in _INHERITED_PYTHON_VARS:
        env.pop(name, None)
    return env


def _oci_auth_mode(args) -> str | None:
    """`aidp.oci_auth`, else `OCI_CLI_AUTH`, else the mode the profile
    implies -- `aidp.oci_profile`, else `OCI_CLI_PROFILE`, else DEFAULT.

    A profile carrying `security_token_file` is a session profile: both CLIs
    need `--auth security_token`, and without it the call is a 401 that reads
    as a permissions problem. `oci` defaults to api_key and `aidp` defaults to
    security_token, so neither default is safe to rely on -- the mode is
    always stated.
    """
    cached = getattr(args, "_oci_auth", None)
    if cached is not None:
        return cached or None
    block = aidp_block(_load_migration_config(args))
    mode = str(block.get("oci_auth") or os.environ.get("OCI_CLI_AUTH")
               or "").strip()
    if not mode:
        mode = _profile_auth_mode(str(block.get("oci_profile") or "").strip()
                                  or os.environ.get("OCI_CLI_PROFILE", "").strip()
                                  or "DEFAULT")
    args._oci_auth = mode
    if mode:
        print(f"  oci auth mode: {mode} (passed as --auth to the `oci` and "
              f"`aidp` CLIs)")
    return mode or None


def _profile_auth_mode(profile: str) -> str:
    """security_token when that profile names a token file, else api_key.

    Read-only, and it reads only the section headers and key NAMES -- never a
    value, so no credential is loaded to decide this.
    """
    path = pathlib.Path(os.environ.get("OCI_CONFIG_FILE")
                        or (pathlib.Path.home() / ".oci" / "config"))
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    current, found = None, False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            current = stripped[1:-1].strip()
            continue
        if current == profile and stripped.split("=", 1)[0].strip() == \
                "security_token_file":
            found = True
    return "security_token" if found else "api_key"


def _oci_runner(args):
    """The `run_process` every transport uses.

    It states the profile and the auth mode on both CLIs -- `oci` takes
    `--profile`, `aidp` takes `-p`, and both take `--auth` -- and hands the
    child an environment that cannot repoint its interpreter.
    """
    import subprocess

    profile = _oci_profile(args)
    mode = _oci_auth_mode(args)

    def run(cmd):
        argv = list(cmd)
        if argv and argv[0] in ("oci", "aidp"):
            if profile and "--profile" not in argv and "-p" not in argv:
                argv[1:1] = ["--profile", profile]
            if mode:
                if "--auth" in argv:
                    argv[argv.index("--auth") + 1] = mode
                else:
                    argv[1:1] = ["--auth", mode]
        return subprocess.run(argv, capture_output=True, text=True,
                              check=False, encoding="utf-8", errors="replace",
                              env=cli_environment(),
                              timeout=DEFAULT_CLI_TIMEOUT)
    return run


def _snowflake_coords(args) -> dict:
    """Source coordinates for a read: the config file, overridden by flags.

    One config file is the documented contract, so every stage that reads
    Snowflake accepts it. An explicit flag still wins -- a one-off run
    against a different role or warehouse should not require editing the
    file -- and the ONLY secret either path carries is a PATH to a
    credential, read at call time.
    """
    config = snowflake_block(_load_migration_config(args))

    def pick(flag: str, key: str | None = None):
        return getattr(args, flag, None) or config.get(key or flag)

    # `--auth` has no argparse default, so None means "not typed" and the
    # flag, the config and then keypair apply in that order. It used to
    # default to keypair and guess "typed" from `"--auth" in sys.argv`,
    # which `--auth=keypair`, the prefix `--au` and main(argv) all defeat:
    # the config's `auth: password` silently won over an explicit flag.
    auth = (getattr(args, "auth", None) or config.get("auth")
            or "keypair")

    return {"auth": str(auth),
            "account": pick("account"), "host": pick("host"),
            "user": pick("user"),
            "role": pick("role"), "warehouse": pick("warehouse"),
            "key_path": pick("key_path"),
            # A secret may be inline now, so it is resolved rather than
            # passed along as a path.
            "password": (resolve_secret(config, "password", "password_path")
                         if config else None),
            "private_key": (resolve_secret(config, "private_key", "key_path")
                            if config and config.get("private_key") else None),
            # From the config only (inline, or a file it names): there is no
            # flag for it, because a passphrase in argv lands in shell
            # history and the process table.
            "key_passphrase": (resolve_secret(config, "key_passphrase",
                                              "key_passphrase_path")
                               if config else None),
            # A PAT may be inline (`token:`) or a path (`pat_path:`),
            # the same as password and private_key. The config already
            # treats `token` as a secret; it was validated and redacted and
            # then never read at connect time.
            "token": (resolve_secret(config, "token", "pat_path")
                      if config and config.get("token") else None),
            "pat_path": pick("pat_path"),
            "password_path": pick("password_path"),
            "database": pick("database")}


def _run_sql_from_args(args):
    coords = _snowflake_coords(args)
    if not coords["account"]:
        raise MissingTarget(
            "no Snowflake account: pass --config (the documented way) or "
            "--account with the other coordinates")

    # `conn.py` reads credentials from PATHS, by design -- that contract is
    # tested and worth keeping. An inline secret is therefore spooled to a
    # 0600 temp file for the life of the call and removed afterwards.
    spooled: list[str] = []

    def as_path(value: str | None, existing: str | None) -> str | None:
        if existing or not value:
            return existing
        fd, path = tempfile.mkstemp(prefix="snowmig_secret_")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(value)
        spooled.append(path)
        return path

    try:
        kwargs = build_connect_kwargs(
            coords["auth"], account=coords["account"], user=coords["user"],
            host=coords.get("host"),
            role=coords["role"], warehouse=coords["warehouse"],
            key_path=as_path(coords.get("private_key"), coords["key_path"]),
            key_passphrase=coords["key_passphrase"],
            pat_path=as_path(coords.get("token"), coords["pat_path"]),
            password_path=as_path(coords.get("password"),
                                  coords["password_path"]))
        conn = connect(**kwargs)
        # Asked for, never assumed: dropping them changes what the whole run
        # can see, so it is the operator's call and it is said out loud.
        if getattr(args, "only_primary_role", False):
            drop_secondary_roles(conn)
            print("  session scoped to its PRIMARY role only (secondary "
                  "roles dropped): every count is what THAT role can see",
                  file=sys.stderr)
        return make_run_sql(conn)
    finally:
        for path in spooled:
            try:
                os.unlink(path)
            except OSError:
                pass


def _mapping_resolution(args) -> dict:
    """Every type-mapping mode with where it came from: `flag`, `config`,
    `default`, or `disabled` (mapping.enabled / --mapping-defaults off)."""
    path = _config_path(args)
    config = load_config(path) if path else {}
    toggle = {"on": True, "off": False}.get(
        getattr(args, "mapping_defaults", None))
    block = mapping_block(config, enabled=toggle)
    written = config.get("mapping") or {}
    out = {"enabled": block["enabled"]}
    for key in ("semi_structured", "timestamp_ntz", "geospatial",
                "source_type_drift"):
        flag = getattr(args, key, None)
        if flag is not None:
            out[key] = {"value": flag, "source": "flag"}
        elif not block["enabled"]:
            out[key] = {"value": block[key], "source": "disabled"}
        else:
            out[key] = {"value": block[key],
                        "source": "config" if key in written else "default"}
    return out


def _mapping(args, key: str) -> str:
    """One resolved mode, said once on stderr so the choice is never silent."""
    res = _mapping_resolution(args)
    print(f'  mapping.{key} = {res[key]["value"]} ({res[key]["source"]}; '
          f'config defaults {"ON" if res["enabled"] else "OFF"})',
          file=sys.stderr)
    return res[key]["value"]


def _decisions(args) -> dict:
    path = _config_path(args)
    return decisions_block(load_config(path) if path else {})


def _refuse_by_decision(args, *, creating: bool, warehouse_clusters: bool = False):
    """Hold an --execute to the decisions recorded in the config. A dry run
    is never refused: it creates nothing."""
    if not getattr(args, "execute", False):
        return
    d = _decisions(args)
    if creating and not d["allow_new_objects"]:
        raise RefusedToExecute(
            "the config records decisions.allow_new_objects: false -- this "
            "migration may not create AIDP objects. Change the decision in "
            "the config, not the command, if that has changed.")
    if warehouse_clusters and d["warehouse_clusters"] == "proposal_only":
        raise RefusedToExecute(
            "the config records decisions.warehouse_clusters: proposal_only -- "
            "S12 proposes warehouse clusters and creates none. See "
            "COMPUTE_PROPOSAL.md.")


def _compute_mode(args) -> dict:
    """`compute:` from the config: new cluster per warehouse, or existing."""
    path = _config_path(args)
    return compute_block(load_config(path) if path else {})


def _assess_inventory(args) -> dict:
    """Seam for tests: patched to avoid a live connection."""
    run_sql = _run_sql_from_args(args)
    databases = args.database or None
    if not databases:
        # The config names the database being migrated; using it means the
        # documented happy path is `assess --connection-config <file>`.
        from_config = _snowflake_coords(args).get("database")
        databases = [lexer.config_name(from_config)] if from_config else None
    inv = build_inventory(
        run_sql, databases,
        row_counts=getattr(args, "row_counts", "metadata"),
        semi_structured=_mapping(args, "semi_structured"),
        geospatial=_mapping(args, "geospatial"),
        timestamp_ntz=_mapping(args, "timestamp_ntz"))
    inv["mapping_resolution"] = _mapping_resolution(args)
    # The census runs in the same pass so the coverage caveat cannot go
    # missing: "N of N objects can move" is only honest next to a statement of
    # what was examined.
    if not getattr(args, "no_census", False):
        inv["census"] = build_census(
            run_sql, inv["databases_in_scope"],
            include_definitions=getattr(args, "capture_definitions", False),
            role=(inv.get("session") or {}).get("ROLE"),
            secondary_roles=secondary_roles_active(
                (inv.get("session") or {}).get("SECONDARY_ROLES")))
    return inv


def cmd_stages(args) -> int:
    """The stage board. Reads artifacts only; touches no environment."""
    out = pathlib.Path(args.out_dir)
    board = build_stage_board(out)
    _write(out, "STAGES.md", render_stages(board))
    phases = phase_report(out)
    _write(out, "phase_report.json", phases)
    _write(out, "PHASES.md", render_phase_report(phases))
    # Coloured by this run; the uncoloured copy embedded in ARCHITECTURE.md
    # is the committed one, and a test holds it equal to the code.
    _write(out, "PHASES.mmd", phase_diagram(board))
    if getattr(args, "write_diagram", False):
        from report.diagram import embed_in_architecture
        path = plugin_root() / "ARCHITECTURE.md"
        path.write_text(embed_in_architecture(
            path.read_text(encoding="utf-8")), encoding="utf-8")
        print(f"  -> {path} (phase diagram)")
    for line in render_stages(board).splitlines():
        print(f"  {line}" if line else "")
    return 0


def census_floor_line(census: dict) -> str:
    """The console line for a census that did not read everything.

    `unreadable` holds a denied kind, a SHOW read at the result cap, and an
    object whose body could not be scanned -- one entry each. Calling every
    entry an unreadable KIND overstated one skipped task body as a whole
    kind lost.
    """
    return (f'  census: {len(census["unreadable"])} read(s) incomplete '
            f'(CENSUS.md, "Could not be read") -- the counts are a floor')


def cmd_assess(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _assess_inventory(args)
    _write(out, "inventory.json", inv)
    _write(out, "INVENTORY.md", render_inventory(inv))
    if inv.get("census"):
        _write(out, "CENSUS.md", render_census(inv["census"]))
        c = inv["census"]
        print(f'  census: {c["total"]} object(s) that are not tables or views '
              f'and cannot migrate')
        if c["unreadable"]:
            print(census_floor_line(c), file=sys.stderr)
    if inv.get("identifier_case_collisions"):
        print("HALT: identifier-case collisions; see INVENTORY.md", file=sys.stderr)
        return HALT
    return 0


def cmd_ingest(args) -> int:
    """Runbook S7 input: the in-AIDP discovery manifest -> inventory.json.

    S6 discovers the estate inside AIDP as a workflow and writes
    `discovery_manifest.json`. Every planning stage reads `inventory.json`.
    This is the bridge, and it calls the SAME type mapper `assess` calls --
    a column planned from a manifest reaches the same verdict as the same
    column planned from a live read.
    """
    from snowflake_source.extract.manifest import inventory_from_manifest

    out = pathlib.Path(args.out_dir)
    manifest_path = pathlib.Path(args.manifest)
    if not manifest_path.exists():
        raise MissingTarget(
            f"no manifest at {manifest_path}. It is written inside the AIDP "
            f"workspace by the discovery workflow (runbook S6); download it "
            f"from backup-snowflake-migration/reports/ first.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    inv = inventory_from_manifest(
        manifest, database=args.database_name,
        semi_structured=_mapping(args, "semi_structured"),
        geospatial=_mapping(args, "geospatial"),
        timestamp_ntz=_mapping(args, "timestamp_ntz"))

    _write(out, "inventory.json", inv)
    _write(out, "INVENTORY.md", render_inventory(inv))

    # `plan` reads dependencies.json, and `deps` needs a live Snowflake
    # session the in-AIDP path does not have. A manifest carries no lineage,
    # and its views are refused for want of SQL, so there is no view->table
    # edge to lose: the empty graph is CORRECT here. It is written with its
    # provenance stated so nobody reads "no edges" as "lineage was checked".
    deps_path = out / "dependencies.json"
    if not deps_path.exists():
        _write(out, "dependencies.json", {
            "edges": [],
            "source_used": "not_extracted",
            "coverage_note":
                "NO lineage was extracted. This inventory came from the "
                "in-AIDP discovery manifest, which records columns and not "
                "view SQL, and ACCOUNT_USAGE was never queried. Tables carry "
                "no inter-table dependency, and views from a manifest are "
                "refused for want of their definition, so no edge is lost by "
                "this being empty -- but an empty graph here is 'not looked "
                "at', not 'looked at and found nothing'. Run `deps` against "
                "a live session if view ordering matters.",
            "unresolved_references": []})
        print("  dependencies.json: written EMPTY with provenance "
              "`not_extracted` -- a manifest carries no lineage")

    inv["mapping_resolution"] = _mapping_resolution(args)
    _write(out, "ingest_result.json", {
        "manifest": str(manifest_path), "database": args.database_name,
        "objects": inv["object_count"],
        "counts_by_type": inv.get("counts_by_type"),
        "extraction_notes": inv.get("extraction_notes") or []})
    print(f'  ingested {inv["object_count"]} object(s) from {manifest_path} '
          f'-- {inv["counts_by_type"]}')

    views = [r for r in inv["inventory"] if r["object_type"] == "VIEW"]
    if views:
        print(f"  note: {len(views)} view(s) came WITHOUT their SQL -- a "
              f"manifest carries columns, not definitions. They will be "
              f"refused by the planner rather than translated blind.",
              file=sys.stderr)
    if inv["extraction_notes"]:
        print(f'  {len(inv["extraction_notes"])} extraction note(s): some '
              f'scope could not be read; absence is not evidence of absence',
              file=sys.stderr)
    if inv.get("identifier_case_collisions"):
        print("HALT: identifier-case collisions; see INVENTORY.md",
              file=sys.stderr)
        return HALT
    return 0


def cmd_deps(args) -> int:
    out = pathlib.Path(args.out_dir)
    deps = extract_dependencies(_run_sql_from_args(args), _read(out, "inventory.json"))
    _write(out, "dependencies.json", deps)
    print(f'  lineage source: {deps["source_used"]}, {len(deps["edges"])} edge(s)')
    if deps.get("warning"):
        print(f'  {deps["warning"]}', file=sys.stderr)
    return 0


def _notebooks_dir() -> pathlib.Path:
    """Where the generated stage notebooks are committed."""
    return (pathlib.Path(__file__).resolve().parents[1]
            / "data-migration-scripts")


def cmd_databases(args) -> int:
    """List the databases the configured role can see (runbook S3).

    A migration registers ONE database as ONE catalog, so the user has to
    pick one before anything is created. That choice used to be made by
    hand-writing a SHOW DATABASES against the source, which left no artifact
    and no record of what was offered; it is a stage so the list is the same
    every time and lands in the run's output like everything else.

    Read-only: SHOW is one of the six verbs the transport permits.
    """
    out = pathlib.Path(args.out_dir)
    rows = _run_sql_from_args(args)("SHOW DATABASES")
    # Databases nobody can migrate: the system application DB, shares, and
    # per-user scratch. Flagged, never hidden -- the operator decides.
    def kind_of(row):
        kind = str(row.get("kind") or "").upper()
        name = str(row.get("name") or "")
        if name == "SNOWFLAKE" or kind == "APPLICATION":
            return "system"
        if "IMPORTED" in kind:
            return "share"
        if name.startswith("USER$") or "PERSONAL" in kind:
            return "personal"
        return "migratable"

    dbs = [{"name": r.get("name"), "kind": r.get("kind"),
            "owner": r.get("owner"), "origin": r.get("origin") or None,
            "category": kind_of(r)} for r in rows]
    result = {"databases": dbs,
              "migratable": [d["name"] for d in dbs
                             if d["category"] == "migratable"]}
    _write(out, "databases.json", result)
    _write(out, "DATABASES.md", render_databases(result))
    print(f'  {len(dbs)} database(s), '
          f'{len(result["migratable"])} migratable')
    return 0


def cmd_catalogs(args) -> int:
    """List the catalogs on the target DataLake, with their real types.

    Read-only, and the answer to "what is actually there" -- which is how
    `catalogType` was found to be INTERNAL/EXTERNAL rather than the
    STANDARD this plugin once sent.
    """
    out = pathlib.Path(args.out_dir)
    coords = _target_coords(args)
    # Listing needs no catalog: demanding one would force the caller to
    # invent a name just to ask which names exist.
    target = resolve_target(**coords, require=("datalake_ocid",))
    backend = args.backend or detect_backend()
    print(f"  backend: {backend}")
    payload = make_call(target, backend=backend,
                        run_process=_oci_runner(args))("list_catalogs")
    cats = [{"name": i.get("displayName"), "key": i.get("key"),
             "catalog_type": i.get("catalogType"),
             "source_type": i.get("sourceType")}
            for i in (payload.get("items") or [])]
    result = {"catalogs": cats,
              "types_seen": sorted({str(c["catalog_type"]) for c in cats})}
    _write(out, "catalogs.json", result)
    _write(out, "CATALOGS.md", render_catalogs(result))
    print(f'  {len(cats)} catalog(s); types: '
          f'{", ".join(result["types_seen"]) or "none"}')
    return 0


def cmd_clean(args) -> int:
    """Remove the default artifact directory, and the demo output beside it.

    Nothing else is touched -- an --out-dir the user chose is theirs, not
    ours to remove, and a demo directory is removed only when it carries the
    demo's `emulation.json` marker.
    """
    import shutil
    target = pathlib.Path(args.out_dir)
    default = default_out_dir()
    # Refuse to delete a directory the operator named. `clean` removing a
    # chosen path would be a data-loss bug wearing a tidy-up costume.
    if target.resolve() != default.resolve():
        print(f"  refusing to delete {target}: `clean` only removes the "
              f"default artifact directory ({default}). Remove a directory "
              f"you chose yourself.", file=sys.stderr)
        return 1
    for path in (default, *_demo_dirs()):
        if path != default and path.exists() and not (
                path / "emulation.json").is_file():
            print(f"  left {path}: it has no emulation.json, so it is not "
                  f"demo output")
        elif path.exists():
            shutil.rmtree(path)
            print(f"  removed {path}")
        else:
            print(f"  nothing at {path}")
    return 0


def _demo_dirs() -> tuple[pathlib.Path, pathlib.Path]:
    """The demo's default output directories, in the working directory."""
    return (pathlib.Path.cwd() / "snowmig_demo",
            pathlib.Path.cwd() / "snowmig_demo_enterprise")


def cmd_build_notebooks(args) -> int:
    """Regenerate the data-plane stage notebooks from their sources.

    The `.ipynb` under `data-migration-scripts/` are GENERATED and committed:
    generated so five copies of the shared helpers cannot drift, committed so
    what ships is reviewable. This is the command that regenerates them.
    """
    from target.stage_notebooks import write_stage_notebooks
    dest = pathlib.Path(args.dest) if args.dest else _notebooks_dir()
    written = write_stage_notebooks(
        dest, pathlib.Path(args.scripts_dir) if args.scripts_dir else None)
    for path in written:
        print(f"  wrote {path}")
    print(f"  {len(written)} notebook(s). They are generated — edit "
          f"engine/dataplane/, not these.")
    return 0


def cmd_maintenance(args) -> int:
    """Snowflake maintenance/layout state. Reports; proposes nothing."""
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    maint = build_maintenance(
        _run_sql_from_args(args), inv,
        history_days=args.history_days,
        probe_table_parameters=args.probe_table_parameters)
    _write(out, "maintenance.json", maint)
    _write(out, "MAINTENANCE.md", render_maintenance(maint))
    flagged = maint["objects_with_signals"]
    print(f'  {flagged} of {len(maint["tables"])} table(s) need a maintenance '
          f'decision; none applied')
    if not maint["account_usage"].get("readable", True):
        print("  ACCOUNT_USAGE unreadable: reclustering and churn NOT measured "
              "(not zero)", file=sys.stderr)
    return 0


def cmd_security(args) -> int:
    """What protects the data today, and what arrives without it."""
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    sec = build_security(_run_sql_from_args(args), inv,
                         include_grants=not args.no_grants)
    _write(out, "security.json", sec)
    _write(out, "SECURITY.md", render_security(sec))
    count = sec["exposure_count"]
    if count is None:
        print("  policy attachments UNREADABLE - exposure is UNKNOWN, not zero",
              file=sys.stderr)
        return 0
    extra = len(sec["secure_views"])
    print(f'  {count} policy exposure(s), {extra} secure view(s) losing SECURE')
    unattached = sec.get("policies_defined_without_attachment") or 0
    policies = sec.get("policies") or {}
    if unattached:
        # SECURITY.md and the stage board already say UNCONFIRMED; the one
        # line the operator reads on the console has to say it too.
        print(f'  {unattached} policy object(s) defined but no attachment '
              'visible (ACCOUNT_USAGE.POLICY_REFERENCES lags ~2 h) - '
              'exposure UNCONFIRMED, re-run before relying on 0',
              file=sys.stderr)
    elif any(not (policies.get(k) or {}).get("readable", True)
             for k in ("masking", "row_access")):
        print('  policy objects could not be enumerated - the empty '
              'attachment list is UNCORROBORATED, see SECURITY.md',
              file=sys.stderr)
    live = sec.get("live_attachments") or {}
    if live.get("attempted") and live.get("failed"):
        # A per-object read that skipped objects has not answered for them,
        # and the count above is not a verdict about those objects.
        print(f'  {len(live["failed"])} object(s) could not be read directly '
              f'({live["probed"]} of {live["objects"]} read) - the count above '
              f'does not speak for them, see SECURITY.md', file=sys.stderr)
    elif live.get("attempted") and not live.get("reason"):
        print(f'  read per object from INFORMATION_SCHEMA '
              f'({live["objects"]} object(s)), so this is current rather than '
              f'subject to the ~2 h ACCOUNT_USAGE lag')
    elif live.get("reason"):
        print(f'  attachments NOT read per object: {live["reason"]}',
              file=sys.stderr)
    if count or extra:
        print("  these objects are created WITHOUT their protection - see "
              "SECURITY.md", file=sys.stderr)
    return 0


def cmd_external_registration(args) -> int:
    """External and Iceberg tables planned `register_in_place`: the AIDP
    registration over OCI Object Storage, generated and never executed.
    Reads Snowflake (SHOW / DESCRIBE / SELECT only) and plan.json."""
    from target.external_registration import (
        build_external_registration, render_external_registration)
    out = pathlib.Path(args.out_dir)
    reg = build_external_registration(_run_sql_from_args(args),
                                      _read(out, "inventory.json"),
                                      _read(out, "plan.json"))
    _write(out, "external_registration.json", reg)
    _write(out, "EXTERNAL_REGISTRATION.md", render_external_registration(reg))
    print(f'  {reg["registrable"]} of {len(reg["tables"])} external/Iceberg '
          f'table(s) have a generated registration; NOTHING executed and no '
          f'bytes moved -- the files must be in OCI Object Storage first '
          f'(EXTERNAL_REGISTRATION.md)')
    if reg.get("after_rewrite"):
        print(f'  {reg["after_rewrite"]} Iceberg table(s) are NOT registrable '
              f'as generated: their register_table CALL names the rewritten '
              f'root metadata file, which exists only after the metadata '
              f'path rewrite (or re-write the table)')
    if reg["not_in_inventory"]:
        print(f'  {len(reg["not_in_inventory"])} external table(s) listed by '
              f'SHOW EXTERNAL TABLES are not in the inventory', file=sys.stderr)
    if reg["unreadable"]:
        print(f'  {len(reg["unreadable"])} read(s) failed; see '
              f'EXTERNAL_REGISTRATION.md', file=sys.stderr)
    return 0


def cmd_share_plan(args) -> int:
    """Outbound shares -> an AIDP Delta Sharing plan. Generated, never
    executed. Reads Snowflake (SHOW SHARES, DESCRIBE SHARE) and plan.json,
    plus security.json when present."""
    from target.share_plan import build_share_plan, render_share_plan
    out = pathlib.Path(args.out_dir)
    security = (_read(out, "security.json")
                if (out / "security.json").is_file() else None)
    sp = build_share_plan(_run_sql_from_args(args),
                          _read(out, "inventory.json"),
                          _read(out, "plan.json"), security=security)
    _write(out, "share_plan.json", sp)
    _write(out, "SHARE_PLAN.md", render_share_plan(sp))
    held = sum(1 for s in sp["shares"] for o in s["objects"]
               if o["status"].startswith("hold"))
    print(f'  {sp["outbound"]} outbound share(s) mapped to a Delta Sharing '
          f'plan; NOTHING executed (SHARE_PLAN.md)')
    if held:
        print(f'  {held} shared table(s) HELD: a policy Delta Sharing does '
              f'not apply, or not checked', file=sys.stderr)
    return 0


def cmd_plan(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    deps = _read(out, "dependencies.json")
    restrictions = None
    if args.restrictions:
        rpath = pathlib.Path(args.restrictions)
        try:
            restrictions = json.loads(rpath.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise InvalidRestriction(
                f"{rpath}: not valid JSON: {exc}") from exc
        # The validator iterates a mapping; anything else is a shape error to
        # report by name, not an AttributeError from inside it.
        if not isinstance(restrictions, dict):
            raise InvalidRestriction(
                f"{rpath}: a restrictions file must be a JSON object mapping "
                f"restriction names to values, got "
                f"{type(restrictions).__name__}")
    # A recorded architecture choice, if the data-options stage has been run.
    choice = None
    if (out / "data_options.json").is_file():
        choice = _read(out, "data_options.json").get("choice")
    try:
        built = build_plan(inv, deps, restrictions=restrictions,
                           bronze_catalog_prefix=args.bronze_catalog_prefix,
        bronze_schema_style=args.bronze_schema_style,
                           architecture_choice=choice,
                           secure_views=args.secure_views)
    except TargetCollision as exc:
        print(f"HALT: {exc}", file=sys.stderr)
        return HALT
    # Carried so the coverage caveat travels with the count it qualifies.
    if inv.get("census"):
        built["census"] = inv["census"]
    _write(out, "plan.json", built)
    _write(out, "PLANNED_OBJECTS.md", render_planned_objects(built))
    s = built["summary"]
    from plan.data_movement import architecture_decision
    decision = architecture_decision(built.get("architecture_choice"))
    if decision["decided"]:
        state = decision["chosen"]["id"]
        custom = decision["chosen"].get("custom_architecture")
        if custom:
            state += f' — "{custom["name"]}" (customer-defined, not assessed)'
    elif decision["deferred"]:
        state = "DEFERRED by the customer — not a gap"
    else:
        state = f'UNDECIDED — {len(decision["options"])} options presented'
    print(f"  architecture: {state}")
    # A restriction's exclusion is the operator's scope, not an object that
    # cannot move; the line said "998 cannot move" for a 4-table canary.
    scoped_out = (s.get("cannot_by_category") or {}).get("restriction", 0)
    rest = s["cannot_migrate"] - scoped_out
    print(f'  planned {s["can_migrate"]} object(s) '
          f'({s["tables"]} table, {s["views"]} view); '
          + (f'{scoped_out} left out by restrictions; ' if scoped_out else "")
          + f'{rest} cannot move')
    if built.get("secure_views_as_views"):
        print(f'  SECURITY WARNING: {len(built["secure_views_as_views"])} '
              f'secure view(s) planned as PLAIN views (--secure-views '
              f'as-view); see PLANNED_OBJECTS.md', file=sys.stderr)
    return 0


def _remap_timestamp_ntz(inv: dict, mode: str) -> tuple[dict, list[str]]:
    """Re-map every TIMESTAMP_NTZ column of `inv` under `mode`, offline.

    Snowflake's default TIMESTAMP is TIMESTAMP_NTZ, the default mapping
    preserves it, and the AIDP metastore refuses it at CREATE TABLE -- so
    almost every real estate halts at `ddl`. The mapping is a decision, not
    an observation, and flipping it must not cost a Snowflake re-read: this
    applies the same mapper `assess`/`ingest` call, so the type and the
    timezone caveat come out identical. A deep copy is returned; the
    inventory on disk is the record of what was observed and stays as it is.
    """
    out = copy.deepcopy(inv)
    changed: list[str] = []
    for rec in out.get("inventory") or []:
        for col in rec.get("columns") or []:
            if str(col.get("DATA_TYPE") or "").strip().upper() != "TIMESTAMP_NTZ":
                continue
            mapped = map_type("TIMESTAMP_NTZ", timestamp_ntz=mode)
            if col.get("target_type") == mapped.spark_type:
                continue
            col["target_type"] = mapped.spark_type
            if mapped.warning:
                # build_create_table lifts "COL: ..." warnings onto the
                # statement, which is how the caveat reaches DDL_PLAN.md.
                caveat = f'{col["COLUMN_NAME"]}: {mapped.warning}'
                rec.setdefault("warnings", []).append(caveat)
            changed.append(f'{rec["source_identifier"]}.{col["COLUMN_NAME"]}')
    out["timestamp_ntz_mode"] = mode
    return out, changed


def cmd_ddl(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    built = _read(out, "plan.json")

    # The TIMESTAMP_NTZ decision can be taken (or re-taken) here, offline.
    # Only the downgrade is applied: `preserve` on an inventory already
    # recorded as `timestamp` is a no-op, because the target refuses NTZ
    # whichever way it was recorded, and re-upgrading would only rebuild
    # the halt.
    remapped: list[str] | None = None
    mode = getattr(args, "timestamp_ntz", None)
    recorded = inv.get("timestamp_ntz_mode")
    if mode == "timestamp" and recorded != "timestamp":
        inv, remapped = _remap_timestamp_ntz(inv, "timestamp")
        print(f"  re-mapped {len(remapped)} TIMESTAMP_NTZ column(s) to "
              f"TIMESTAMP offline (inventory.json and INVENTORY.md are "
              f"untouched and still show the preserved type)")
    elif mode == "timestamp":
        remapped = []
    elif mode == "preserve":
        remapped = []
        if recorded == "timestamp":
            print("  note: inventory.json was recorded with --timestamp-ntz "
                  "timestamp; `preserve` does not re-upgrade it here. The "
                  "target refuses TIMESTAMP_NTZ either way -- not "
                  "re-upgraded.", file=sys.stderr)

    payload = build_ddl_payload(inv, built)
    # Read by the copy stage, which has no config of its own: a source
    # column whose live type is not the one its spec was planned for is
    # refused, or converted and recorded `verified_with_conversion`.
    drift = _mapping_resolution(args)["source_type_drift"]
    payload["source_type_drift"] = drift["value"]
    print(f'  mapping.source_type_drift = {drift["value"]} '
          f'({drift["source"]}): what the copy does with a source column '
          f'whose type changed after this plan')
    if remapped is not None:
        payload["timestamp_ntz_mode"] = (
            "timestamp" if mode == "timestamp" else recorded)
        payload["remapped_columns"] = remapped
    _write(out, "ddl_plan.json", payload)
    _write(out, "DDL_PLAN.md", render_ddl_plan(payload))
    # The session-wide view of every translation, written beside the DDL it
    # describes so the two always come from the same inputs.
    tmap = build_translation_map(inv, built, payload)
    _write(out, "translation_map.json", tmap)
    _write(out, "TRANSLATION_MAP.md", render_translation_map(tmap))
    t = tmap["totals"]
    print(f'  translation map: {t["columns"]} column(s), '
          f'{t["distinct_source_types"]} type(s); views '
          f'{t["views_translated"]} translated, {t["views_verbatim"]} '
          f'verbatim, {t["views_refused"]} refused')

    # HALT on a type the target will refuse. The artifacts are written first
    # on purpose: the plan is still worth reading, and the operator needs to
    # see WHICH columns are at fault. Exit 3 is the established halt code --
    # a condition to resolve with the user, never one to pick a winner on.
    rejected = payload.get("target_rejected") or []
    if rejected:
        cols = sum(len(r["columns"]) for r in rejected)
        print(f"  HALT: {cols} column(s) in {len(rejected)} table(s) use a "
              f"type the target refuses at CREATE TABLE.", file=sys.stderr)
        for r in rejected[:5]:
            print(f"    {r['target_fqn']}: {', '.join(r['columns'])} "
                  f"-> {r['type']}", file=sys.stderr)
        if len(rejected) > 5:
            print(f"    ... and {len(rejected) - 5} more table(s)",
                  file=sys.stderr)
        for remedy in dict.fromkeys(r["remedy"] for r in rejected):
            print(f"  {remedy}", file=sys.stderr)
        print("  Nothing was created. Re-run `ddl --timestamp-ntz timestamp` "
              "(offline), or fix the INPUT and re-run `ddl` -- do not hand "
              "this plan to the structure workflow.", file=sys.stderr)
        return HALT
    return 0


def cmd_deploy(args) -> int:
    out = pathlib.Path(args.out_dir)
    ddl_plan = _read(out, "ddl_plan.json")
    built = _read(out, "plan.json")
    inv = _read(out, "inventory.json") if (out / "inventory.json").exists() else {}
    target = None
    if args.execute:
        target = resolve_target(**_target_coords(args))
        backend = args.backend or detect_backend()
        print(f"  backend: {backend} · transport: {args.transport}")

    # Pre-flight FIRST, always: both ends are known here and nothing has been
    # created yet. Written and echoed so it cannot be skipped.
    preflight = render_preflight(
        built,
        source=({"account": (inv.get("session") or {}).get("A"),
                 "region": (inv.get("session") or {}).get("R"),
                 "role": (inv.get("session") or {}).get("ROLE"),
                 "databases": inv.get("databases_in_scope")} if inv else None),
        target=(dataclasses.asdict(target) if target is not None else None))
    _write(out, "PREFLIGHT.md", preflight)
    print()
    for line in preflight.splitlines():
        print(f"  {line}" if line else "")
    print()

    # After the pre-flight, so re-reading PREFLIGHT.md still works; before
    # the result is built, so nothing below can replace the executed record.
    if not args.execute and _executed_record_exists(out, "deploy_result.json"):
        return _refuse_dry_run_overwrite(out, "deploy_result.json", "deploy")

    if args.transport == "catalog_api":
        # The working transport. `POST .../sql/execute` returns 404, and a
        # structure-only clone needs no Spark cluster anyway.
        call = (make_call(target, backend=args.backend or detect_backend(),
                          run_process=_oci_runner(args))
                if args.execute else None)
        result = deploy_catalog(ddl_plan, target=target,
                                execute=args.execute, call=call,
                                diagnose=not args.no_diagnose)
    else:
        run_sql = (make_aidp_run_sql(target,
                                     backend=args.backend or detect_backend(),
                                     run_process=_oci_runner(args))
                   if args.execute else None)
        result = deploy(ddl_plan, target=target, execute=args.execute,
                        run_sql=run_sql, chunk_size=args.chunk_size)
    _write(out, "deploy_result.json", result)
    _write(out, "SOFT_CLONE_SUMMARY.md", render_soft_clone_summary(built, result))
    if result.get("matched_nothing"):
        print(f'  NOTHING MATCHED: all '
              f'{result.get("out_of_scope_count", 0)} planned object(s) '
              f'target '
              + ", ".join(result.get("out_of_scope_catalogs") or [])
              + f', not `{result.get("catalog_in_scope")}`. Nothing was '
                f'created. Point --catalog at the catalog the plan targets, '
                f'or re-plan with --bronze-catalog-prefix.', file=sys.stderr)
        return 1
    return 1 if result.get("failed") or result.get("chunk_errors") \
        or result.get("mismatched_targets") else 0


def cmd_run(args) -> int:
    """Run an AIDP job as a WORKFLOW, poll it, and bring back its output.

    Runbook S6 and S10 execute inside AIDP, never on the operator's machine.
    A workflow run is the unit of evidence: it is logged, re-runnable, and its
    task output is exportable. An interactive notebook leaves none of that.

    A budget that runs out is reported as STILL RUNNING. It is never rounded
    to success and never rounded to failure.
    """
    from target.provisioning import make_provision_call
    from target.jobs import TERMINAL_STATES, watch_job

    out = pathlib.Path(args.out_dir)
    # Flags first, then the config's `aidp:` block, like every other AIDP
    # stage. This read the flags only, so the README's `run --job <name>`
    # after filling in aidp.workspace failed with "needs --datalake-ocid"
    # in a directory where `catalogs` resolved all four coordinates.
    coords = _target_coords(args)
    ocid = coords["datalake_ocid"]
    if not ocid:
        raise MissingTarget(
            "run needs the aiDataPlatform OCID: the workflow executes inside "
            "AIDP. Put it under `aidp.datalake_ocid` in the config, or pass "
            "--datalake-ocid.")
    args.workspace = coords["workspace"]
    if not args.workspace:
        # Named, never used: a record in this out dir may be another
        # migration's, and a stale record must not redirect a job run.
        prov = (_read(out, "provision_result.json")
                if _executed_record_exists(out, "provision_result.json")
                else None) or {}
        recorded = (prov.get("workspace") or {}).get("key")
        raise MissingTarget(
            "run needs the workspace: a job run belongs to one workspace. "
            "Put its key under `aidp.workspace` in the config, or pass "
            "--workspace."
            + (f" provision_result.json here records workspace "
               f"{(prov.get('workspace') or {}).get('name')} with key "
               f"{recorded} -- pass --workspace {recorded} if that is this "
               f"migration's." if recorded else
               " provision records it as workspace.key in "
               "provision_result.json."))

    parameters = {}
    for pair in (args.param or []):
        if "=" not in pair:
            raise MissingTarget(
                f"--param expects name=value, got {pair!r}")
        name, value = pair.split("=", 1)
        parameters[name] = value

    if parameters:
        # REFUSE, rather than accept-and-discard. A RUN-level `parameters`
        # is taken by the run API and was probed live to reach the notebook
        # neither as argv nor as environment; the route a stage notebook
        # does read -- oidlUtils.parameters.getParameter -- is live-verified
        # for a job TASK's parameters only, not a run's. So a `--param
        # schema=SALES` would start a run that quietly ignored it. A scope
        # flag that silently does nothing is worse than one that is missing:
        # it reads as applied.
        # Only a name some stage declares is offered as a --stage-param:
        # provision refuses any other, so suggesting it would send the
        # operator to a second refusal. The name is qualified with the job's
        # stage -- a per-schema copy job (snowmig_02_copy_<schema>) is the
        # copy_schema stage -- because an unqualified name goes to every
        # stage declaring it, and `mode` means different things to 01 and
        # 02. A job that is no stage gets `<stage>.<name>` for a name more
        # than one stage declares.
        from target.provisioning import COPY_JOB_PREFIX
        from target.stage_notebooks import STAGES, declared_stage_params
        head = (
            "--param is refused: a run-level job parameter does not reach "
            "a notebook stage. The stage notebooks read the job TASK's "
            "parameters (oidlUtils.parameters.getParameter) and their "
            "PARAMS cell. This run would ignore " + ", ".join(sorted(parameters)) + " and execute "
            "what the job already carries: its task parameters, which win "
            "over the notebook's PARAMS cell, then the PARAMS literals.\n")
        stage = next((s for s in STAGES if s.job == args.job), None)
        per_schema = bool(stage is None and args.job
                          and args.job.startswith(COPY_JOB_PREFIX))
        if per_schema:
            stage = next(s for s in STAGES if s.key == "copy_schema")
        by_name = declared_stage_params()
        declared = list(stage.params) if stage else sorted(by_name)

        def _qualified(name: str) -> str:
            if stage:
                return f"{stage.key}.{name}"
            return (f"<stage>.{name}" if len(by_name.get(name, ())) > 1
                    else name)

        # On a per-schema copy job `schema` and `tables` are set on that
        # job's task, never baked: the job passes its own `schema`, and a
        # baked copy_schema.tables would narrow EVERY per-schema copy job
        # through the one shared 02 notebook -- provision refuses it once
        # there are two copy schemas, so advising it would send the
        # operator into that second refusal.
        task_only = {"schema", "tables"} if per_schema else set()
        known = sorted(n for n in parameters if n in declared
                       and n not in task_only)
        unknown = sorted(n for n in parameters if n not in declared)
        scoped = (
            f"`schema` is the task parameter of {args.job}: the job is "
            f"already scoped to its schema, and that task parameter wins "
            f"over any PARAMS literal, so no --stage-param changes it. To "
            f"copy another schema run that schema's own job "
            f"(snowmig_02_copy_<schema>); a schema with no job is added by "
            f"re-planning and re-pushing.\n"
            if per_schema and "schema" in parameters else "")
        scoped += (
            f"`tables` narrows {args.job} only as a task parameter on that "
            f"job's task: add `tables=<value>` to its task in the console "
            f"(a task parameter wins over the PARAMS literal). A baked "
            f"copy_schema.tables would narrow every per-schema copy job "
            f"through the one shared 02_copy_schema notebook, which "
            f"provision refuses with two or more copy schemas.\n"
            if per_schema and "tables" in parameters else "")
        route = (
            "  * re-run `provision --execute --reuse-existing "
            "--refresh-notebooks "
            + " ".join(f"--stage-param {_qualified(name)}=<value>"
                       for name in known)
            + "` -- it rewrites each stage notebook's PARAMS cell and "
            "uploads it (console edits to that cell are lost)"
            + ("; <stage> is one of " + ", ".join(s.key for s in STAGES)
               if not stage and any("<stage>" in _qualified(n)
                                    for n in known) else "")
            + (". The one 02_copy_schema notebook backs every per-schema "
               "copy job, so a value baked there applies to all of them"
               if stage and stage.key == "copy_schema" else "")
            + ", or\n" if known else "")
        undeclared = (
            (f"{stage.notebook_name} does not declare " if stage
             else "No stage notebook declares ")
            + ", ".join(unknown) + ", so no route sets it. Declared names: "
            + ", ".join(declared) + ".\n" if unknown else "")
        if not (known or unknown):
            raise MissingTarget(head + scoped.rstrip("\n"))
        raise MissingTarget(
            head + scoped + undeclared
            + "Set stage parameters where they are actually read:\n"
            + route
            + "  * set a task parameter of that name on the job's task, or "
            "edit the PARAMS cell of "
            "backup-snowflake-migration/scripts/<stage>.ipynb, in the "
            "console.\n"
            "Scope is an INPUT either way -- never edit the stage logic to "
            "make it cover less.")


    call = make_provision_call(ocid, run_process=_oci_runner(args))

    job_key = args.job_key
    if not job_key:
        if not args.job:
            raise MissingTarget("run needs --job <name> or --job-key <key>.")
        payload = call("list_jobs", workspace=args.workspace)
        items = (payload.get("items") if isinstance(payload, dict)
                 else None) or []
        matches = [j for j in items
                   if str(j.get("displayName") or j.get("name")) == args.job]
        if not matches:
            names = ", ".join(sorted(str(j.get("displayName") or j.get("name"))
                                     for j in items)) or "(none)"
            raise MissingTarget(
                f"no job named {args.job!r} in workspace {args.workspace}. "
                f"Jobs present: {names}. Provision creates the migration "
                f"jobs; this stage only runs one.")
        if len(matches) > 1:
            raise MissingTarget(
                f"{len(matches)} jobs are named {args.job!r}; pass --job-key "
                f"to say which. Ambiguity is not resolved by guessing.")
        job_key = str(matches[0].get("key") or matches[0].get("id"))

    parameters: dict[str, str] = {}

    print(f"  workflow: job={args.job or job_key} key={job_key}")
    if parameters:
        print(f"  parameters: {parameters}")

    def _on_poll(status: str, attempt: int) -> None:
        print(f"    poll {attempt}: {status}", flush=True)

    def _on_restart(stale: str, fresh: str) -> None:
        print(f"    cold start: the cluster did not pick up run {stale} "
              f"within {args.cold_start_seconds:.0f}s (its task never "
              f"started). Cancelled it; resubmitted as {fresh}.", flush=True)

    # Every run key is printed the moment it exists. Once a run is submitted
    # it is the operator's evidence, whatever the watch does next: a watch
    # that raised used to leave the key only inside an echoed GET URI.
    submitted: list[str] = []

    def _on_submit(run_key: str) -> None:
        submitted.append(run_key)
        print(f"  submitted: run {run_key}", flush=True)
        # Recorded the moment it exists, as RUNNING. The record used to be
        # written only when the watch ended, so for the whole run the board
        # said NOT_RUN and offered this stage -- and `deploy` -- as
        # unblocked: an invitation to a second, concurrent write (live,
        # 2026-09-29, during the structure job).
        _record({"run_key": run_key, "status": "RUNNING",
                 "message": "submitted; snowmig run is watching it",
                 "output": "", "terminal": False, "restarts": [],
                 "polls": 0, "unrecognised": False,
                 "status_unreadable": False, "cancel_unconfirmed": False,
                 "ok": False, "watching": True,
                 "task_parameters_check": task_check})

    slug = (args.job or job_key).replace("/", "_")

    def _record(result: dict) -> None:
        result["job"] = args.job
        result["job_key"] = job_key
        result["workspace"] = args.workspace
        result["parameters"] = parameters
        result["submitted_runs"] = list(submitted)
        _write(out, f"run_{slug}.json", result)
        _write(out, f"RUN_{slug}.md", _render_run(result))

    task_check = None
    refreshing = bool(getattr(args, "refresh", False)
                      or getattr(args, "run_key", None))
    # Stamped before the first submission: a manifest discovery wrote for
    # THIS run cannot be older than this (see fetch_discovery_manifest).
    # A --refresh submits nothing, so it has no such time and the
    # manifest's freshness is not checked against one.
    run_started = (None if refreshing
                   else datetime.datetime.now(datetime.timezone.utc))
    if refreshing:
        # Re-read a run that exists; submit, cancel and resubmit nothing.
        from target.jobs import refresh_run
        prior_path = out / f"run_{slug}.json"
        prior = (_read(out, f"run_{slug}.json")
                 if prior_path.is_file() else {}) or {}
        run_key = args.run_key or prior.get("run_key")
        if not run_key:
            raise MissingTarget(
                f"--refresh needs a run: no run_{slug}.json records one. Pass "
                f"--run-key <key> (a run started from the console has no "
                f"local record; its key is in the console's run list).")
        print(f"  refresh: run {run_key} (nothing is submitted, cancelled or "
              f"resubmitted)")
        result = refresh_run(call, workspace=args.workspace, run_key=run_key,
                             poll_seconds=args.poll_seconds,
                             max_polls=args.max_polls, on_poll=_on_poll)
        seen = result.pop("job_key_seen", None)
        if seen and job_key and str(seen) != str(job_key):
            raise MissingTarget(
                f"run {run_key} belongs to job {seen}, not {args.job or ''} "
                f"({job_key}); its record was NOT written, so one job's run "
                f"is never filed under another's.")
        if prior.get("run_key") == run_key:
            # The same run: its cold-start history is still its evidence.
            result["restarts"] = prior.get("restarts") or []
            submitted.extend(prior.get("submitted_runs") or [])
        result["refreshed_at"] = datetime.datetime.now(
            datetime.timezone.utc).isoformat()
    else:
        result = None
        task_check = _check_task_parameters(call, workspace=args.workspace,
                                            job_key=job_key, job=args.job)
    try:
        if result is None:
            result = watch_job(call, workspace=args.workspace, job_key=job_key,
                               parameters=parameters or None,
                               poll_seconds=args.poll_seconds,
                               max_polls=args.max_polls, on_poll=_on_poll,
                               cold_start_seconds=args.cold_start_seconds,
                               cold_start_restarts=args.cold_start_restarts,
                               on_restart=_on_restart, on_submit=_on_submit)
    except Exception as exc:
        if not submitted:
            # Nothing reached AIDP, so the transport's own message is true.
            raise
        # A run WAS submitted. Its record is written, and the message says
        # so: "nothing was sent ... re-run this stage" about a run that may
        # be copying rows right now sends the operator to start another.
        text = str(exc)[:300]
        _record({"run_key": submitted[-1], "status": "UNREADABLE",
                 "message": text, "output": "", "terminal": False,
                 "restarts": [], "polls": None, "unrecognised": False,
                 "status_unreadable": True, "status_error": text,
                 "cancel_unconfirmed": False, "ok": False})
        print(f"  {slug}: run {submitted[-1]} WAS submitted, but the watch "
              f"stopped: {text}\n  It may still be running. Check it in the "
              f"console before anything else; do not start another run "
              f"until it has ended. The record is RUN_{slug}.md.",
              file=sys.stderr)
        return 1
    if task_check is not None:
        result.setdefault("task_parameters_check", task_check)
    _record(result)

    from report.stages import run_case
    case = run_case(result)
    if case == "unreadable":
        print(f'  {slug}: run {result["run_key"]} was submitted, but its '
              f'status could not be read ({result.get("status_error")}). It '
              f'may still be running: check it in the console, and do not '
              f'start another run until it has ended.', file=sys.stderr)
        return 1
    if case in ("cold_start_exhausted", "unrecognised", "cancel_unconfirmed",
                "still_running"):
        exhausted = result.get("cold_start_exhausted")
        if case == "cold_start_exhausted":
            # Not "still running": the cluster ignored every attempt. The
            # last run was cancelled so it does not hold the job's slot.
            from report.stages import cold_start_outcome
            outcome = cold_start_outcome(exhausted)
            if outcome == "cancelled":
                last = "cancelled."
            elif outcome == "ended":
                # Ended on its own before the cancel landed: it RAN.
                last = (f'NOT cancelled: it ended {exhausted.get("cancel_state")}'
                        f' on its own before the cancel landed, so it RAN. Read '
                        f'its output (RUN_{slug}.md) before any re-run -- a '
                        f're-run of an append copy writes the rows twice.')
            else:
                last = (f'NOT confirmed cancelled ({exhausted.get("cancel_state")}'
                        f'); cancel it by hand (`aidp workflow cancel-job-run '
                        f'{args.workspace} {exhausted["run"]}`).')
            print(f'  {slug}: COLD START — the cluster did not pick up any of '
                  f'{_runs_submitted(result)} run(s) in time, each '
                  f'given {args.cold_start_seconds:.0f}s. The last, '
                  f'{exhausted["run"]}, was ' + last
                  + (" Check the cluster in the console (state, recent "
                     "restarts), then re-run; raise --cold-start-restarts if "
                     "it simply needs more attempts." if outcome == "cancelled"
                     else ""), file=sys.stderr)
            return 1
        if case == "unrecognised":
            # Neither a verdict nor "still going": a status this plugin does
            # not classify. Saying STILL RUNNING here would round it up.
            print(f'  {slug}: UNRECOGNISED STATE {result["status"]} after '
                  f'{result.get("polls")} poll(s) — not a status this plugin '
                  f'knows, so neither done nor still running. Check the run '
                  f'in the console and report the status so it can be '
                  f'classified.')
            return 1
        if case == "cancel_unconfirmed":
            # The watchdog fired but the cancel never reached a terminal
            # state, so nothing was resubmitted: the slot is still held by a
            # run the cluster may never pick up. That is not "still running"
            # in the healthy sense, and not a verdict either.
            print(f'  {slug}: cold start suspected; cancel unconfirmed after '
                  f'{args.max_polls} poll(s). Run {result["run_key"]} was '
                  f'never confirmed cancelled, so nothing was resubmitted. '
                  f'Cancel it by hand (`aidp workflow cancel-job-run '
                  f'{args.workspace} {result["run_key"]}`), then re-run.')
            return 1
        print(f"  {slug}: STILL RUNNING after {args.max_polls} poll(s) — "
              f"not failed, not done. Re-check with the run key above.")
        return 0
    verdict = "SUCCESS" if result["ok"] else result["status"]
    print(f"  {slug}: {verdict}")
    if not result["ok"] and result.get("error_trace"):
        print(f'  task error: {" ".join(result["error_trace"].split())[:300]}',
              file=sys.stderr)
        if result.get("platform_transient"):
            print(f"  {PLATFORM_TRANSIENT_HINT}", file=sys.stderr)
    if result["ok"] and args.job == "snowmig_00_discover":
        # Runbook S7 starts from this manifest; bring it down now, through
        # the console's own download route, rather than leave the operator
        # to find a way. A failed download does not unmake the discovery:
        # the manifest is safe on the workspace, and `fetch` retries it.
        stage_params = _provisioned_stage_params(out)
        remote = manifest_remote(stage_params)
        try:
            got = fetch_discovery_manifest(
                call, workspace=args.workspace, out=out,
                submitted_at=run_started, stage_params=stage_params)
            if got["fresh"] is False:
                print(f'  manifest NOT this run\'s: {got["message"]}',
                      file=sys.stderr)
            else:
                print(f'  {got["message"]}; next: `ingest --manifest '
                      f'{got["dest"]} --database-name <SOURCE_DB>`')
        except Exception as exc:
            print(f"  manifest NOT fetched ({str(exc)[:200]}); it is on the "
                  f"workspace at {remote}. Run `fetch` to retry.",
                  file=sys.stderr)
    return 0 if result["ok"] else 1


def _check_task_parameters(call, *, workspace: str, job_key: str,
                           job: str | None) -> None:
    """Refuse a run whose job TASK parameters no stage would read, BEFORE a
    run is paid for (a job run costs minutes of start-up).

    A task parameter reaches the notebook by name. A name matching none of
    a declared parameter's spellings (param_spellings) is read by nothing:
    `dryRn=true` leaves `dry-run` at its default False -- a real write. A
    value the stage would refuse (`mode=apend`) fails inside the notebook
    after the start-up. Both are refused here with what was meant. When the
    job definition cannot be read, or carries no task parameters, nothing
    is refused and that is said: not checked is not "checked and fine".

    Returns what was established, for the run record ({"checked": bool,
    "parameters" | "reason"}), or None for a job that is not a stage job.
    A check that passed used to print nothing, and one that could not read
    the parameter list returned silently -- the very case this docstring
    promised to name.
    """
    import difflib
    from target.provisioning import COPY_JOB_PREFIX, _listed_task_parameters
    from target.stage_notebooks import (STAGES, check_stage_params,
                                        param_spellings)
    stage = next((s for s in STAGES if job and s.job == job), None)
    if stage is None and job and job.lower().startswith(COPY_JOB_PREFIX):
        stage = next(s for s in STAGES if s.key == "copy_schema")
    if stage is None:
        return
    try:
        job_def = call("get_job", workspace=workspace,
                       job_key=job_key) or {}
        params = _listed_task_parameters(job_def)
        tasks = job_def.get("tasks")
        if (params is None and isinstance(tasks, list) and tasks
                and isinstance(tasks[0], dict)
                and tasks[0].get("parameters") is None):
            # Live (2026-09-29): a task with no parameters answers
            # `parameters: null` -- none, not unreadable.
            params = {}
    except Exception as exc:
        reason = f"the job definition could not be read ({str(exc)[:160]})"
        print(f"  task parameters NOT checked: {reason}", file=sys.stderr)
        return {"checked": False, "reason": reason}
    if params is None:
        reason = ("the job definition carries no readable task parameter "
                  "list (tasks[0].parameters)")
        print(f"  task parameters NOT checked: {reason}", file=sys.stderr)
        return {"checked": False, "reason": reason}
    if not params:
        print(f"  task parameters: none on the job's task; the "
              f"{stage.key} stage runs on its PARAMS defaults")
        return {"checked": True, "parameters": {}}
    canonical = {sp: name for name in stage.params
                 for sp in param_spellings(name)}
    unknown, bad = [], []
    for name, value in params.items():
        if name not in canonical:
            near = difflib.get_close_matches(
                name.lower().replace("_", "-"), list(stage.params), n=1)
            unknown.append(f"`{name}`" + (f" (did you mean `{near[0]}`?)"
                                          if near else ""))
            continue
        try:
            check_stage_params({f"{stage.key}.{canonical[name]}": value})
        except ValueError as exc:
            bad.append(f"`{name}={value}`: {str(exc)[:200]}")
    if unknown or bad:
        raise MissingTarget(
            f"job {job} carries task parameter(s) its notebook would not "
            f"read as meant, so NO run was submitted:\n"
            + "".join(f"  * {u} is not a parameter of the {stage.key} stage "
                      f"-- no lookup reads it, so the stage would run on its "
                      f"default\n" for u in unknown)
            + "".join(f"  * {b}\n" for b in bad)
            + f"Parameters this stage reads: "
              f"{', '.join(sorted(stage.params))}. Fix the job's task "
              f"parameters in the console (or re-run provision), then run "
              f"again.")
    shown = ", ".join(f"{k}={v}" for k, v in sorted(params.items()))
    print(f"  task parameters checked: {shown} -- each read by the "
          f"{stage.key} stage")
    return {"checked": True, "parameters": dict(params)}


PLATFORM_TRANSIENT_HINT = (
    "This is the AIDP job runner losing contact with the cluster (a "
    "transient service error), not a failure of the stage's own code. The "
    "stage's reports record what it finished; re-run the same job to resume "
    "from them, and run reconcile to see what the stopped run left.")


def _runs_submitted(result: dict) -> int:
    """How many runs were really submitted. Not `len(restarts) + 1`: a
    restart whose cancel was not confirmed submitted nothing."""
    if result.get("submitted_runs"):
        return len(result["submitted_runs"])
    return 1 + sum(1 for r in result.get("restarts") or []
                   if r.get("new_run"))


def _render_run(result: dict) -> str:
    """The workflow run as evidence: what ran, what it returned, its log."""
    from report.stages import run_case
    polls = result.get("polls", "?")
    case = run_case(result)
    if case == "unreadable":
        verdict = (f"**STATUS COULD NOT BE READ** — run "
                   f"`{result.get('run_key')}` was submitted, but its status "
                   f"could not be read"
                   + (f" after {polls} poll(s)" if polls else "")
                   + f": {result.get('status_error')}. It may still be "
                   f"running. This is neither success nor failure: check it "
                   f"in the console, and do not start another run until it "
                   f"has ended.")
    elif case == "cold_start_exhausted":
        ex = result["cold_start_exhausted"]
        error = (f", error: {ex.get('cancel_error')}"
                 if ex.get("cancel_error") else "")
        head = (f"**COLD START — attempts exhausted.** The cluster did not "
                f"pick up any of {_runs_submitted(result)} "
                f"run(s); the last, `{ex.get('run')}`, sat "
                f"{ex.get('after_seconds'):.0f}s with its task unstarted ")
        # The same test the console applies. A cancel that did not reach a
        # terminal state leaves a run that may still hold the job's only
        # slot -- or start later, unwatched -- so "nothing ran, re-run" is
        # only said when the cancel is confirmed.
        from report.stages import cold_start_outcome
        outcome = cold_start_outcome(ex)
        if outcome == "cancelled":
            verdict = (head + f"and was then cancelled (cancel state "
                       f"`{ex.get('cancel_state')}`{error}). Nothing ran. "
                       f"Check the cluster, then re-run.")
        elif outcome == "ended":
            # The task started after the pick-up check and ENDED before the
            # cancel landed: the run did its work (or failed doing it).
            verdict = (head + f"at the last check, then **ended "
                       f"`{ex.get('cancel_state')}` on its own before the "
                       f"cancel landed**{error}. It RAN: read its output "
                       f"below before any re-run -- a re-run of an append "
                       f"copy writes the rows twice.")
        else:
            verdict = (head + f"and was **NOT confirmed cancelled** (state "
                       f"`{ex.get('cancel_state')}`{error}). It may still "
                       f"run. Cancel it by hand (`aidp workflow "
                       f"cancel-job-run {result.get('workspace')} "
                       f"{ex.get('run')}`) and check the cluster before "
                       f"re-running.")
    elif case == "unrecognised":
        verdict = (f'**UNRECOGNISED STATE `{result.get("status")}`** — after '
                   f'{polls} poll(s) the run reports a status this plugin '
                   f'classifies as neither running nor ended. This is neither '
                   f'success nor failure; check the run in the console.')
    elif case == "cancel_unconfirmed":
        verdict = (f"**STILL RUNNING — cold start suspected; cancel "
                   f"unconfirmed.** The cluster had not picked up run "
                   f"`{result.get('run_key')}`, the cancel did not reach a "
                   f"terminal state (see below), so nothing was resubmitted "
                   f"and the poll budget ({polls} poll(s)) ran out with it "
                   f"still `{result.get('status')}`. Cancel it by hand and "
                   f"re-run; this is neither success nor failure.")
    elif case == "still_running" and result.get("watching"):
        verdict = ("**RUNNING** — submitted, and `snowmig run` is watching "
                   "it; this record is rewritten when the run ends. If that "
                   "command was interrupted, bring the record up to date with "
                   "`snowmig run --job <job> --refresh` -- never start "
                   "another run meanwhile.")
    elif case == "still_running":
        verdict = (f"**STILL RUNNING** — the poll budget ({polls} poll(s)) "
                   f"ran out with the job still `{result.get('status')}`. "
                   f"This is neither success nor failure; re-check the run "
                   f"key.")
    elif case == "success":
        verdict = "**SUCCESS**"
    else:
        verdict = f'**{result.get("status")}** — {result.get("message") or "no message"}'

    lines = [
        f'# Workflow run — `{result.get("job") or result.get("job_key")}`',
        "",
        verdict,
        "",
        "| | |",
        "|---|---|",
        f'| workspace | `{result.get("workspace")}` |',
        f'| job key | `{result.get("job_key")}` |',
        f'| run key | `{result.get("run_key")}` |',
        f'| status | `{result.get("status")}` |',
    ]
    check = result.get("task_parameters_check")
    if isinstance(check, dict):
        if not check.get("checked"):
            seen = f'NOT checked — {check.get("reason")}'
        elif check.get("parameters"):
            seen = "checked before submitting — " + ", ".join(
                f"`{k}={v}`" for k, v in sorted(check["parameters"].items()))
        else:
            seen = "checked before submitting — none on the task"
        lines.append(f"| task parameters | {seen} |")
    lines.append("")
    if result.get("error_trace"):
        # The fetched log is only the head of a long notebook's output, so
        # the cause of a failure is here, from the task run, not below.
        lines += ["## Task error trace", ""]
        if result.get("platform_transient"):
            lines += [PLATFORM_TRANSIENT_HINT, ""]
        lines += ["```", result["error_trace"], "```", ""]
    if result.get("parameters"):
        lines += ["Parameters:", ""]
        lines += [f'- `{k}` = `{v}`' for k, v in result["parameters"].items()]
        lines.append("")
    # A restart is part of the record. The run that produced the output below
    # is NOT the run that was first submitted, and a reader comparing this
    # report against the console has to be able to see that.
    if result.get("restarts"):
        lines += [
            "## Cold-start restarts",
            "",
            "The cluster did not pick up the run(s) below — the job run sat "
            "at `RUNNING` with its task never started. A run whose cancel "
            "reached a terminal state was resubmitted, and **the output "
            "below then belongs to the last run key, not the first.** A "
            "run whose cancel did NOT (it raised, or never left CANCELING) "
            "was kept, and no new run was submitted while it may still "
            "hold the job's slot.",
            "",
            "| Run | Cancelled to | Waited | Outcome |",
            "|---|---|---|---|",
        ]
        for r in result["restarts"]:
            if r.get("new_run"):
                outcome = f'resubmitted as `{r.get("new_run")}`'
                run = r.get("abandoned_run")
            else:
                outcome = ("kept — cancel unconfirmed"
                           + (f': {r.get("cancel_error")}'
                              if r.get("cancel_error") else ""))
                run = r.get("kept_run")
            lines.append(f'| `{run}` | `{r.get("cancel_state")}` | '
                         f'{r.get("after_seconds"):.0f}s | {outcome} |')
        lines.append("")
    lines += ["## Output", "", "```", (result.get("output") or "(none)").strip(),
              "```", ""]
    return "\n".join(lines)


def _catalog_record_names(name: str) -> tuple[str, str]:
    """The per-catalog record files: catalog_result_<name>.json, CATALOG_<name>.md."""
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(name)).strip("_") or "catalog"
    return f"catalog_result_{slug}.json", f"CATALOG_{slug}.md"


def _catalog_summary(res: dict) -> dict:
    """What the board and the next catalog run carry of one executed catalog."""
    test = res.get("test_connection") or {}
    return {"catalog": res.get("catalog"),
            "catalog_type": res.get("catalog_type"),
            "action": res.get("action"), "key": res.get("key"),
            "verified": res.get("verified"),
            "test_connection": ({"status": test.get("status"),
                                 "error": test.get("error")}
                                if test else None)}


def _catalogs_created_here(out: pathlib.Path,
                           datalake_ocid: str | None) -> list[str]:
    """The keys and names of every catalog the resource ledger records this
    migration CREATING -- on this aiDataPlatform, when the row says which.
    `catalog --execute` reuses an existing catalog only if it is one of
    these, or --reuse-existing is passed."""
    from report.resources import LEDGER
    path = out / LEDGER
    names: list[str] = []
    if not path.is_file():
        return names
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("kind") != "catalog" or row.get("action") != "created":
            continue
        if (datalake_ocid and row.get("datalake_ocid")
                and row["datalake_ocid"] != datalake_ocid):
            continue
        names += [str(v) for v in (row.get("key"), row.get("name")) if v]
    return names


def cmd_catalog(args) -> int:
    """Register the target catalog. EXTERNAL/SNOWFLAKE by default.

    STANDARD is allowed and is step S4 of the runbook (S3 registers the
    EXTERNAL source). Creating the catalog
    is not the same as creating its tables: the catalog is ONE control-plane
    object, while a table create through the same API can return 202 Accepted
    and silently create nothing. So the container is made here and the tables
    are made on compute, by the structure workflow (S10).
    """
    _refuse_by_decision(args, creating=True)
    out = pathlib.Path(args.out_dir)
    # The CLI vocabulary is the runbook's ("standard"); the wire value is
    # INTERNAL. Translating here means the alias can never reach the API,
    # which rejects catalogType=STANDARD outright.
    requested_type = args.catalog_type.upper()
    catalog_type = normalize_catalog_type(requested_type)

    if catalog_type == "INTERNAL":
        alias = " (sent as INTERNAL, which is what the API calls it)" \
            if requested_type != catalog_type else ""
        print(f"  note: creating the {requested_type} catalog CONTAINER "
              f"only{alias}. Its schemas and tables are created on AIDP "
              f"compute by the structure workflow (runbook S10), where each "
              f"create is read back.")

    # The connection comes from the ONE config file, discovered the same way
    # every other stage discovers it -- requiring an explicit --config here
    # meant "just run it from the config" silently did nothing.
    connection = None
    config_path = _config_path(args, required=bool(args.execute))
    if config_path:
        block = snowflake_block(_load_migration_config(args))
        connection = build_snowflake_connection_details(block)
        # Only an EXTERNAL registration reads the Snowflake block; the note
        # was printed for the INTERNAL container too (S4), where no part of
        # the connection is used at all.
        if block.get("schema") and catalog_type == "EXTERNAL":
            print(f'  note: `schema: {block["schema"]}` is not used here — an '
                  f'EXTERNAL catalog registers the whole database '
                  f'({block.get("database")}). It scopes the source side.')

    # The name to register, resolved ONCE and keyed strictly by catalog type:
    # `aidp.external_catalog` is the Snowflake source's EXTERNAL catalog,
    # `aidp.catalog` the INTERNAL target. Neither stands in for the other --
    # ensure_catalog reuses ANY catalog carrying the name, whatever its type,
    # so registering the source under the target's name would make the later
    # `--catalog-type standard` step silently "reuse" the EXTERNAL one.
    coords = _target_coords(args)
    aidp = aidp_block(_load_migration_config(args)) if config_path else {}
    key = "external_catalog" if catalog_type == "EXTERNAL" else "catalog"
    name = args.catalog or aidp.get(key)
    if not name or not str(name).strip():
        raise MissingTarget(
            f"catalog name not supplied: pass --catalog <name> or set "
            f"`aidp.{key}:` in {config_path or 'the migration config'}")
    name = str(name).strip()

    # S3 (EXTERNAL) and S4 (INTERNAL) are two runs of this one stage. Each
    # catalog keeps its own record, catalog_result_<name>.json and
    # CATALOG_<name>.md; catalog_result.json is the latest EXECUTED one and
    # carries `catalogs_recorded`, every catalog registered so far. With one
    # shared file, the S4 dry run was refused (the S3 record is evidence)
    # and the S4 --execute then overwrote that evidence (live, 2026-09-29).
    own_json, own_md = _catalog_record_names(name)
    latest = _read(out, "catalog_result.json") \
        if _executed_record_exists(out, "catalog_result.json") else None
    if not args.execute:
        if _executed_record_exists(out, own_json):
            return _refuse_dry_run_overwrite(out, own_json, "catalog")
        if latest is not None and latest.get("catalog") == name:
            return _refuse_dry_run_overwrite(out, "catalog_result.json",
                                             "catalog")
        result = {"dry_run": True, "catalog": name,
                  "catalog_type": catalog_type,
                  "source_type": args.source_type.upper(),
                  "connection_config": (str(config_path) if config_path
                                        else None),
                  "connection_fields": sorted(connection or {})}
    else:
        # The Target carries the name being registered, not the config's
        # INTERNAL `catalog:`, which need not exist yet at this step.
        target = resolve_target(**{**coords, "catalog": name})
        backend = args.backend or detect_backend()
        print(f"  backend: {backend}")
        result = ensure_catalog(
            display_name=name,
            call=make_call(target, backend=backend,
                           run_process=_oci_runner(args)),
            catalog_type=catalog_type, source_type=args.source_type.upper(),
            connection=connection,
            description=args.description or
            f"Snowflake {name}, registered by the snowflake-migrator",
            created_here=_catalogs_created_here(out, coords["datalake_ocid"]),
            reuse_existing=getattr(args, "reuse_existing", False))
        result["dry_run"] = False
        result["source_type"] = args.source_type.upper()

    # Validation as a COMMAND, not a suggestion: the documented
    # POST /actions/testConnection (live-verified 2026-09-16), then the
    # async operation it names polled through /asyncOperations to a verdict.
    if args.test_connection and not args.execute:
        print("  test-connection: skipped — it needs an existing catalog "
              "(the API resolves RBAC on the key), so it only runs with "
              "--execute")
    elif args.test_connection:
        if connection is None:
            raise ConnectionConfigError(
                "--test-connection needs --connection-config: the API "
                "requires the connection details inline, not just the "
                "catalog key")
        ocid = coords["datalake_ocid"]
        if not ocid:
            raise MissingTarget(
                "--test-connection needs the aiDataPlatform OCID: put it "
                "under `aidp:` in the config, or pass --datalake-ocid")
        from target.provision_api import build_test_connection_body
        from target.provisioning import (make_provision_call,
                                         connection_test_outcome)
        pcall = make_provision_call(ocid, run_process=_oci_runner(args))
        # The API resolves the catalog KEY (RBAC DESCCATALOG), which is what
        # ensure_catalog reported back -- not necessarily the display name.
        probe = pcall("test_connection",
                      body=build_test_connection_body(
                          str(result.get("key") or name),
                          source_type=args.source_type.upper(),
                          connection_properties=connection,
                          display_name=name))
        # The response body is empty and the async key rides in a response
        # HEADER, which the transport keeps under `_headers`;
        # connection_test_outcome reads it from wherever the envelope put it
        # and polls /asyncOperations/{key} to a verdict. PENDING means the
        # budget ran out, never "not looked"; a missing key is said to be
        # missing.
        outcome = connection_test_outcome(pcall, probe)
        result["test_connection"] = outcome
        print(f'  test-connection: {outcome["status"]}'
              + (f' — {outcome.get("error")}' if outcome.get("error") else "")
              + (f' — {outcome.get("note")}' if outcome.get("note") else ""))

    if args.execute:
        recorded = [c for c in ((latest or {}).get("catalogs_recorded")
                                or ([_catalog_summary(latest)] if latest
                                    else []))
                    if c.get("catalog") != name]
        result["catalogs_recorded"] = recorded + [_catalog_summary(result)]
    _write(out, own_json, result)
    _write(out, own_md, render_catalog(result))
    if args.execute or latest is None:
        _write(out, "catalog_result.json", result)
        _write(out, "CATALOG.md", render_catalog(result))
    else:
        print(f"  note: catalog_result.json keeps the executed record of "
              f"{latest.get('catalog')}; this dry run is in {own_json}")
    # The per-catalog records keep each report; the ledger is the record of
    # every catalog this migration allocated (for billing).
    if args.execute and result.get("action") in ("created", "reused"):
        from report.resources import record_resource
        record_resource(out, stage="catalog", kind="catalog",
                        name=name, type=catalog_type,
                        key=result.get("key") or name,
                        action=result.get("action"),
                        datalake_ocid=coords["datalake_ocid"])
    print(f'  catalog {name}: '
          f'{"dry run — nothing created" if not args.execute else result["action"]}')
    return 0


def cmd_compute(args) -> int:
    out = pathlib.Path(args.out_dir)
    warehouses = extract_warehouses(_run_sql_from_args(args))
    _write(out, "warehouses.json", warehouses)
    mode = _compute_mode(args)
    sizing = propose_all(warehouses["warehouses"],
                         credit_price_usd=args.credit_price,
                         mode=mode["warehouse_clusters"],
                         existing_cluster_id=mode["cluster_id"])
    sizing["metering_source"] = warehouses["metering_source"]
    sizing["metering_note"] = warehouses["metering_note"]
    _write(out, "compute.json", sizing)
    _write(out, "COMPUTE_PROPOSAL.md", render_compute(sizing))
    print(f'  {warehouses["warehouse_count"]} warehouse(s), '
          f'metering: {warehouses["metering_source"]}')
    return 0


def _target_coords(args) -> dict:
    """The four AIDP coordinates: flags first, then the config file.

    `target/coords.py` still performs no I/O -- it is handed values and
    cannot discover them -- so the file can only ever ADD a default here,
    where `_aidp_from_config` prints what it contributed.
    """
    block = _aidp_from_config(args)
    coords = {
        "datalake_ocid": (getattr(args, "datalake_ocid", None)
                          or block.get("datalake_ocid")),
        "workspace": getattr(args, "workspace", None) or block.get("workspace"),
        "cluster_id": (getattr(args, "cluster_id", None)
                       or block.get("cluster_id")),
        "catalog": getattr(args, "catalog", None) or block.get("catalog"),
    }
    # The config's contribution is announced by _aidp_from_config; a value
    # given as a flag was not shown anywhere, so the operator never saw the
    # whole destination before an --execute (runbook rule 6).
    flagged = {k: v for k, v in coords.items()
               if v and getattr(args, k, None)}
    if flagged and not getattr(args, "_flags_announced", False):
        print("  destination from flags: "
              + ", ".join(f"{k}={v}" for k, v in sorted(flagged.items())))
        args._flags_announced = True
    return coords


def _optional_target(args):
    """Resolve a target only if all four coordinates are known."""
    coords = _target_coords(args)
    if not all(coords.values()):
        return None
    return resolve_target(**coords)


def cmd_smoke(args) -> int:
    out = pathlib.Path(args.out_dir)
    target = _optional_target(args)
    dest_call = None
    if target is not None:
        backend = args.backend or detect_backend()
        print(f"  destination backend: {backend}")
        # The catalog API, not SQL: `POST .../sql/execute` returns 404, so a
        # SQL-based check reported FAIL against a working destination.
        dest_call = make_call(target, backend=backend,
                              run_process=_oci_runner(args))
    # The probe WRITES (one schema, created and removed), so it is gated
    # like every other write: `--write-probe` alone is a dry run that says
    # what it would create; `--write-probe --execute` creates it.
    write_probe = bool(args.write_probe and args.execute)
    if args.write_probe and not args.execute:
        if target is not None:
            print(f"  dry run: --write-probe would create and remove one "
                  f"schema named snowmig_permission_probe_<hex> in "
                  f"{target.catalog} on {target.datalake_ocid} "
                  f"(workspace {target.workspace}); add --execute to run it. "
                  f"Nothing was created.")
        else:
            print("  dry run: --write-probe needs the four AIDP coordinates "
                  "and --execute; nothing was created.")
    result = run_smoke(source_run_sql=_run_sql_from_args(args), target=target,
                       dest_call=dest_call, write_probe=write_probe,
                       database=(args.database or [None])[0]
                       if getattr(args, "database", None) else None)
    if args.write_probe and not args.execute \
            and not result["destination"].get("skipped"):
        result["destination"]["write_note"] = (
            "not attempted: --write-probe was given without --execute, so "
            "this was a dry run. Re-run with --write-probe --execute to "
            "create one probe schema and remove it again.")
    _write(out, "smoke.json", result)
    _write(out, "SMOKE_TEST.md", render_smoke(result))
    verdict = smoke_verdict(result)
    if verdict == "PARTIAL":
        print("  verdict: PARTIAL — Snowflake was checked; the AIDP destination "
              f'was NOT ({result["destination"].get("reason", "")}). Not a pass.')
    else:
        print(f"  verdict: {verdict}")
    return 0 if verdict == "PASS" else 1


def cmd_notebook(args) -> int:
    out = pathlib.Path(args.out_dir)
    ddl_plan = _read(out, "ddl_plan.json")
    built = _read(out, "plan.json")
    inv = _read(out, "inventory.json")
    session = inv.get("session", {})

    # Do not pick a catalog on the user's behalf when there is a choice. One
    # notebook per catalog, and which one is a decision, not a default.
    candidates = built.get("catalogs_to_create") or []
    if args.catalog:
        catalog = args.catalog
    elif len(candidates) == 1:
        catalog = candidates[0]
    elif not candidates:
        raise ValueError("no catalog to generate a notebook for")
    else:
        raise ValueError(
            "the plan spans " + str(len(candidates)) + " catalogs ("
            + ", ".join(candidates) + "); pass --catalog to choose one. One "
            "notebook per catalog, and the choice is not assumed.")

    doc = build_notebook(ddl_plan, built, catalog=catalog,
                         source={"account": session.get("A"),
                                 "region": session.get("R")})
    local = out / f"snowmig_shallow_clone_{catalog}.ipynb"
    _write(out, local.name, doc)
    ws_path = notebook_workspace_path(catalog)

    lines = [f"# Shallow-clone notebook for `{catalog}`", "",
             f"Generated: `{local}`", f"Intended AIDP path: `{ws_path}`", "",
             f'{doc["metadata"]["snowmig"]["statement_count"]} statement(s); '
             f'{doc["metadata"]["snowmig"]["blocked_count"]} object(s) not '
             "attempted.", "",
             "**The notebook creates empty structure and moves no data.** It "
             "prints progress per object so a long run stays visible, and "
             "verifies each object individually at the end.", ""]

    if not args.upload:
        lines += ["Not uploaded. The structure it describes is created on AIDP "
                  "compute by the `provision` + `run` workflow (runbook S10); "
                  "`--upload` is a dry run without `--execute`, and is refused "
                  "with it (see below).", ""]
        _write(out, "NOTEBOOK.md", "\n".join(lines))
        print("  generated only; the structure workflow is `provision` + `run`")
        return 0

    target = _optional_target(args)
    if target is None:
        raise MissingTarget(
            "--upload needs all four AIDP coordinates: --datalake-ocid, "
            "--workspace, --cluster-id, --catalog. Ask the user for them.")

    # An upload is a write, so it is a dry run without --execute like every
    # other write. And WITH --execute it is refused: stage notebooks reach
    # the workspace through the upload surface `provision` drives, each
    # read back; this command keeps no second upload route.
    remedy = (f"The notebook is at {local}. Upload it from the workspace UI, "
              f"or create the structure through the workflow path: `snowmig "
              f"provision --execute` places the stage notebooks in the "
              f"workspace and `snowmig run --job snowmig_01_structure` "
              f"executes the structure stage from ddl_plan.json (S10).")
    if not args.execute:
        print(f"  dry run: --upload would place {local.name} at {ws_path} in "
              f"workspace {target.workspace} on {target.datalake_ocid}; "
              f"nothing was sent. With --execute the upload is refused. "
              f"{remedy}")
        lines += [f"Upload was a **dry run**: nothing was sent to `{ws_path}`. "
                  f"With `--execute` the upload is refused. {remedy}", ""]
        _write(out, "NOTEBOOK.md", "\n".join(lines))
        return 0

    lines += [f"**Upload refused.** Nothing was sent to `{ws_path}`: this "
              f"command does not upload notebooks; the stage notebooks are "
              f"placed on the workspace by `provision`. {remedy}", ""]
    _write(out, "NOTEBOOK.md", "\n".join(lines))
    print(f"error: notebook --upload --execute is refused; nothing was sent. "
          f"{remedy}", file=sys.stderr)
    return 1


def cmd_summary(args) -> int:
    out = pathlib.Path(args.out_dir)
    built = _read(out, "plan.json")
    inv = _read(out, "inventory.json")
    deployed = None
    if (out / "deploy_result.json").is_file():
        deployed = _read(out, "deploy_result.json")
    target = _optional_target(args)
    ddl_payload = (_read(out, "ddl_plan.json")
                   if (out / "ddl_plan.json").is_file() else None)
    tmap = build_translation_map(inv, built, ddl_payload)
    _write(out, "translation_map.json", tmap)
    _write(out, "TRANSLATION_MAP.md", render_translation_map(tmap))
    tokens = build_token_report(
        out, in_flight=_in_flight(args, "summary"),
        exclude=_windows(getattr(args, "exclude_window", None)))
    _write(out, "tokens.json", tokens)
    _write(out, "TOKENS.md", render_tokens(tokens))
    from report.resources import build_resources
    resources = build_resources(out)
    _write(out, "resources.json", resources)
    security = (_read(out, "security.json")
                if (out / "security.json").is_file() else None)
    summary = render_summary(built, inv, deployed,
                             dataclasses.asdict(target) if target else None,
                             translation_map=tmap, resources=resources,
                             security=security)
    # Last on purpose: the cost of the run is read after what the run did.
    summary = summary.rstrip() + "\n\n" + "\n".join(tokens_section(tokens))
    _write(out, "SUMMARY.md", summary.rstrip() + "\n")
    return 0


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _in_flight(args, stage: str) -> dict:
    """The stage now running, so its own tokens are counted up to now."""
    return {"stage": stage,
            "started_at": getattr(args, "_started_at", None) or _now(),
            "ended_at": _now(),
            "claude_session_id": os.environ.get("CLAUDE_CODE_SESSION_ID")}


def _reporting(args) -> dict:
    path = _config_path(args)
    return reporting_block(load_config(path) if path else {})


def _publish_stage(args) -> None:
    """The accumulated report, to the migration workspace, after a stage.

    Only when `reporting.publish_each_stage` is on and provision has created
    this run's workspace. Never raises: a report that cannot be uploaded
    must not fail the migration step it describes."""
    if os.environ.get("SNOWMIG_NO_STAGE_PUBLISH"):
        return
    # Only a migration PHASE ends with a published report; a utility
    # (stages, build-notebooks, catalogs, ...) is a tool, not a step.
    from report.stages import STAGES
    if _log_name(args) not in {s["stage"] for s in STAGES}:
        return
    try:
        path = _config_path(args)
        config = load_config(path) if path else {}
        rep = reporting_block(config)
        if not rep["publish_each_stage"]:
            return
        if not decisions_block(config)["allow_new_objects"]:
            return
        out = pathlib.Path(args.out_dir)
        prov = (json.loads((out / "provision_result.json").read_text())
                if (out / "provision_result.json").is_file() else {})
        workspace = (prov.get("workspace") or {}).get("key")
        ocid = (getattr(args, "datalake_ocid", None)
                or aidp_block(config).get("datalake_ocid"))
        if prov.get("dry_run") or not workspace or not ocid:
            return
        from report.stage_output import publish_stage_output
        from target.provisioning import make_provision_call
        res = publish_stage_output(
            make_provision_call(ocid, run_process=_oci_runner(args)),
            workspace, out, rep["workspace_dir"])
        total = len([s for s in res["steps"] if s["file"]])
        print(f'  stage report -> workspace {rep["workspace_dir"]}/: '
              f'{res["verified"]}/{total} file(s) read back '
              f'(snapshot {res["snapshot"]})', file=sys.stderr)
    except Exception as exc:
        print(f"  stage report NOT published: {str(exc)[:200]}",
              file=sys.stderr)


def _record_retries(out_dir, stage: str, events: list[dict]) -> None:
    """Every retry of this stage, one line each, for the phase report."""
    if not events:
        return
    try:
        with (pathlib.Path(out_dir) / "retries.jsonl").open(
                "a", encoding="utf-8") as fh:
            for event in events:
                fh.write(json.dumps({"stage": stage, **event}) + "\n")
    except OSError:
        pass


def _log_name(args) -> str:
    """The phase this invocation is, as the run log records it."""
    return stage_for(args.cmd, getattr(args, "job", None))


def cmd_teardown(args) -> int:
    """Terminate the compute this migration allocated. Dry run unless
    --execute; only clusters recorded in provision_result.json."""
    from target.provisioning import make_provision_call
    from target.teardown import render_teardown, teardown
    out = pathlib.Path(args.out_dir)
    prov = _read(out, "provision_result.json")
    path = _config_path(args)
    config = load_config(path) if path else {}
    action = args.action or teardown_block(config)["action"]
    ocid = args.datalake_ocid or aidp_block(config).get("datalake_ocid")
    if args.execute and not ocid:
        raise MissingTarget("teardown --execute needs --datalake-ocid (or "
                            "aidp.datalake_ocid in the config)")
    call = (make_provision_call(ocid, run_process=_oci_runner(args))
            if args.execute else None)
    scope = getattr(args, "scope", None) or "compute"
    if scope != "compute":
        return _teardown_scoped(args, out, prov, call, ocid, scope)
    if getattr(args, "include_data", False):
        raise MissingTarget("--include-data belongs to --scope all (it lets "
                            "that scope delete the INTERNAL catalog and its "
                            "tables); the compute scope deletes no data")
    res = teardown(call, prov, action=action, execute=args.execute,
                   datalake_ocid=ocid)
    _write(out, "teardown_result.json", res)
    _write(out, "TEARDOWN.md", render_teardown(res))
    targets = [s for s in res["steps"]
               if str(s.get("action")).startswith("would ")]
    for s in res["steps"]:
        if s.get("action") in ("key_unknown", "provenance_unknown"):
            # A create this migration asked for whose key was never seen, or
            # a cluster a pre-provenance record cannot place: never touched.
            print(f'  teardown: {s.get("name")} '
                  f'({s.get("cluster") or "key never recorded"}): '
                  f'{s.get("detail")}', file=sys.stderr)
    if res.get("unknown"):
        # Could not tell is not "nothing to do".
        print(f'  teardown: {res["note"]}', file=sys.stderr)
        return 1
    if res["dry_run"]:
        print(f"  teardown: dry run — would {action} {len(targets)} "
              f"cluster(s); nothing changed")
        return 0
    # Every step counts, a cluster without a key included: could not
    # identify is not "nothing to do".
    print(f'  teardown: {res["verified"]}/{len(res["steps"])} cluster(s) '
          f'{action} verified')
    return 0 if res["verified"] == len(res["steps"]) else 1


def _teardown_scoped(args, out, prov, call, ocid, scope: str) -> int:
    """teardown --scope credential | all: remove the credential, or undo
    the migration. Opt-in; the default scope keeps the migration's output."""
    from report.resources import LEDGER
    from target.teardown import render_teardown, teardown_everything
    if scope == "all" and args.action == "stop":
        raise MissingTarget("--scope all deletes; --action stop contradicts "
                            "it. Use the default scope to stop the clusters "
                            "and keep everything else.")
    if args.include_data and scope != "all":
        raise MissingTarget("--include-data belongs to --scope all")
    ledger = []
    path = out / LEDGER
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                ledger.append(json.loads(line))
            except ValueError:
                continue
    res = teardown_everything(call, prov or {}, scope=scope,
                              execute=args.execute, ledger=ledger,
                              include_data=args.include_data,
                              datalake_ocid=ocid)
    _write(out, "teardown_result.json", res)
    _write(out, "TEARDOWN.md", render_teardown(res))
    if res.get("unknown"):
        print(f'  teardown: {res["note"]}', file=sys.stderr)
        return 1
    if res["dry_run"]:
        todo = [s for s in res["steps"] if s.get("action") == "would delete"]
        print(f"  teardown --scope {scope}: dry run — would delete "
              f"{len(todo)} object(s): "
              + ", ".join(f'{s["kind"]} {s.get("name") or s.get("catalog")}'
                          for s in todo)
              + ("; kept: " + str(len(res["kept"])) if res["kept"] else ""))
        return 0
    print(f'  teardown --scope {scope}: {res["verified"]}/'
          f'{len(res["steps"])} deletion(s) verified')
    return 0 if res["verified"] == len(res["steps"]) else 1


# What `fetch` brings down when no --path is given, and what `run` fetches
# by itself after a discovery that SUCCEEDED: the manifest runbook S7 reads.
DISCOVERY_MANIFEST_REMOTE = ("backup-snowflake-migration/reports/"
                             "discovery_manifest.json")

# How far the cluster's clock may run behind the laptop's before a manifest
# stamped just after the run was submitted reads as older than it.
MANIFEST_CLOCK_SKEW = datetime.timedelta(minutes=10)


def manifest_remote(stage_params: dict | None) -> str:
    """Where discovery wrote its manifest: the `reports-dir` the stage was
    provisioned with (plain or `discover.`-qualified), else the default.

    The fetch after discovery used to read the default folder whatever the
    stage had been given -- live, a run with reports_r5 downloaded an
    earlier run's manifest from reports/ and called it this run's.
    """
    params = stage_params or {}
    rd = params.get("discover.reports-dir") or params.get("reports-dir")
    if not rd:
        return DISCOVERY_MANIFEST_REMOTE
    rel = str(rd).strip().lstrip("/")
    if rel.startswith("Workspace/"):
        rel = rel[len("Workspace/"):]
    return f"{rel.rstrip('/')}/discovery_manifest.json"


def _provisioned_stage_params(out: pathlib.Path) -> dict:
    """The stage params the last executed provision applied, if recorded."""
    try:
        rec = json.loads((out / "provision_result.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return dict(rec.get("stage_params") or {})


def fetch_discovery_manifest(call, *, workspace: str, out: pathlib.Path,
                             submitted_at: datetime.datetime | None,
                             stage_params: dict | None, download=None) -> dict:
    """Bring down THIS run's discovery manifest, or say why it is not.

    Downloaded to a side name first. It becomes `discovery_manifest.json`
    only when its `generated_at` is not older than the run's submission
    (allowing for clock skew); an older one is an earlier run's discovery
    and is kept as `discovery_manifest.STALE.json`, never over the local
    manifest. No timestamp: saved, and said to be unchecked.
    """
    if download is None:
        from target.provisioning import download_ws_file as download
    remote = manifest_remote(stage_params)
    dest = out / "discovery_manifest.json"
    incoming = out / "discovery_manifest.incoming.json"
    got = download(call, workspace=workspace, path=remote, dest=incoming)
    try:
        stamp = json.loads(incoming.read_text(encoding="utf-8")).get("generated_at")
        generated = datetime.datetime.fromisoformat(stamp) if stamp else None
    except (OSError, ValueError, TypeError):
        generated = None
    if generated is not None and generated.tzinfo is None:
        generated = generated.replace(tzinfo=datetime.timezone.utc)
    if (generated is not None and submitted_at is not None
            and generated < submitted_at - MANIFEST_CLOCK_SKEW):
        stale = out / "discovery_manifest.STALE.json"
        incoming.replace(stale)
        return {"remote": remote, "dest": str(stale), "size": got.get("size"),
                "fresh": False,
                "message": (f"the manifest at {remote} predates this run "
                            f"(generated {generated.isoformat()}, run "
                            f"submitted {submitted_at.isoformat()}): it is an "
                            f"earlier discovery, kept as {stale.name} and NOT "
                            f"saved as discovery_manifest.json. Check the "
                            f"stage's reports-dir, then `fetch` again.")}
    incoming.replace(dest)
    note = ("; its freshness was not checked (a --refresh submits no run "
            "to compare it with)" if submitted_at is None and generated is not None
            else "" if generated is not None else
            "; its freshness could not be checked (no generated_at in it)")
    return {"remote": remote, "dest": str(dest), "size": got.get("size"),
            "fresh": (True if generated is not None and submitted_at is not None
                      else None),
            "message": f"manifest fetched -> {dest} ({got.get('size')} bytes){note}"}


def cmd_fetch(args) -> int:
    """Download one file from the migration workspace into the out dir.

    Runbook S7 starts from the manifest the discovery WORKFLOW wrote inside
    AIDP. This is how it comes down: the console's own download route, the
    bytes checked against the size the server reports. Read-only on AIDP.
    """
    from target.provisioning import download_ws_file, make_provision_call

    out = pathlib.Path(args.out_dir)
    coords = _target_coords(args)
    if not coords["datalake_ocid"] or not coords["workspace"]:
        raise MissingTarget(
            "fetch needs the aiDataPlatform OCID and the workspace key: put "
            "them under `aidp.datalake_ocid` and `aidp.workspace` in the "
            "config, or pass --datalake-ocid and --workspace.")
    # Default: where discovery was provisioned to write it, not a fixed
    # folder that may hold an earlier run's manifest.
    remote = (args.path or manifest_remote(_provisioned_stage_params(out))).lstrip("/")
    if remote.startswith("Workspace/"):
        # The cluster sees the tree under /Workspace; the API wants it
        # relative. Accept the path either way it is copied from a log.
        remote = remote[len("Workspace/"):]
    dest = out / (args.to or remote.rsplit("/", 1)[-1])
    call = make_provision_call(coords["datalake_ocid"],
                               run_process=_oci_runner(args))
    res = download_ws_file(call, workspace=coords["workspace"], path=remote,
                           dest=dest)
    print(f'  fetched {res["path"]} -> {res["dest"]} ({res["size"]} bytes)')
    return 0


def cmd_publish(args) -> int:
    """Copy the finished report into the migration's workspace folder.
    Dry run unless --execute; every file is read back."""
    from target.provisioning import make_provision_call
    from target.publish import publish_report
    out = pathlib.Path(args.out_dir)
    coords = _target_coords(args)
    ocid, workspace = coords.get("datalake_ocid"), coords.get("workspace")
    if not ocid or not workspace:
        raise MissingTarget("publish needs --datalake-ocid and --workspace "
                            "(or both in the config's aidp: block)")
    call = (make_provision_call(ocid, run_process=_oci_runner(args))
            if args.execute else None)
    res = publish_report(call, workspace, out, execute=args.execute)
    _write(out, "publish_result.json", res)
    if res["dry_run"]:
        print(f'  publish: dry run — {len(res["steps"])} file(s) would go to '
              f'{res["folder"]}; nothing uploaded')
        return 0
    total = len([s for s in res["steps"] if s["file"]])
    print(f'  publish: {res["verified"]}/{total} file(s) read back in '
          f'{res["folder"]}')
    if res["not_verified"]:
        print(f'  NOT verified: {", ".join(res["not_verified"])}',
              file=sys.stderr)
        return 1
    return 0


def cmd_jobs(args) -> int:
    """Generated jobs for what Snowflake refreshed or scheduled.

    Offline by default: reads plan.json and inventory.json, writes
    generated_jobs.json, GENERATED_JOBS.md and one notebook per task under
    generated_jobs/, and touches nothing. `--register` creates the jobs in
    AIDP through the provisioning calls `provision` uses -- UNSCHEDULED:
    the source cadence is recorded, never applied.
    """
    from report.render import render_generated_jobs
    from target.generated_jobs import (JOB_PREFIX, NOTEBOOK_DIR,
                                       build_generated_jobs,
                                       register_generated_jobs)

    out = pathlib.Path(args.out_dir)
    if (getattr(args, "register", False)
            and not _decisions(args)["allow_new_objects"]):
        # Held to the config's decisions exactly as an --execute is, and
        # before anything is written, so a refused run leaves no half-state.
        raise RefusedToExecute(
            "the config records decisions.allow_new_objects: false -- this "
            "migration may not create AIDP objects, and --register creates "
            "jobs. Nothing was generated or registered; `jobs` without "
            "--register writes the files offline.")
    plan = _read(out, "plan.json")
    inventory = _read(out, "inventory.json")
    res = build_generated_jobs(plan, inventory)

    folder = out / NOTEBOOK_DIR
    folder.mkdir(parents=True, exist_ok=True)
    wanted = set()
    for rel, notebook in sorted(res["notebooks"].items()):
        path = out / rel
        path.write_text(json.dumps(notebook, indent=1), encoding="utf-8")
        wanted.add(path.name)
    # This generator's own notebooks from an earlier run, for a job this run
    # no longer generates. Left, they would read as current.
    for stale in sorted(folder.glob(f"{JOB_PREFIX}*.ipynb")):
        if stale.name not in wanted:
            stale.unlink()
            print(f"  removed stale {stale}")

    payload = {k: v for k, v in res.items() if k != "notebooks"}
    payload["notebook_files"] = sorted(res["notebooks"])
    payload["generated_at"] = _now()
    payload["registered"] = False
    code = 0
    if getattr(args, "register", False):
        coords = _target_coords(args)
        # From a flag or the config only, as for every later command: never
        # implicitly from provision_result.json, so a record from another
        # migration cannot redirect the write (README, Hand-off).
        workspace, cluster = coords["workspace"], coords["cluster_id"]
        if not coords["datalake_ocid"] or not workspace or not cluster:
            raise MissingTarget(
                "jobs --register needs the aiDataPlatform OCID, the "
                "workspace key and the cluster key the jobs run on: pass "
                "--datalake-ocid, --workspace and --cluster-id, or put them "
                "under `aidp:` in the config (`provision --execute` prints "
                "the keys in its hand-off block). Nothing was registered.")
        from target.provisioning import make_provision_call
        call = make_provision_call(coords["datalake_ocid"],
                                   run_process=_oci_runner(args))
        reg = register_generated_jobs(call, workspace=workspace,
                                      cluster_key=cluster,
                                      jobs=res["jobs"], notebook_dir=out)
        payload["registered"] = True
        payload["registration"] = reg
        # The ledger is the record of what this migration allocated;
        # generated_jobs.json is rewritten by every run, so a job the first
        # --register created would drop out of it. `teardown --scope all`
        # reads these rows (a create not yet confirmed is tried too).
        from report.resources import record_resource
        leaves = {t["notebook"].rsplit("/", 1)[-1]: j["name"]
                  for j in res["jobs"] for t in j["tasks"]}
        for name in reg["created"] + reg["unconfirmed"]:
            record_resource(out, stage="jobs", kind="job", name=name,
                            workspace=workspace,
                            action=("created" if name in reg["created"]
                                    else "create_requested"))
        for leaf, job_name in leaves.items():
            if job_name in reg["created"] + reg["unconfirmed"]:
                record_resource(out, stage="jobs", kind="ws_object",
                                name=f'{reg["folder"]}/{leaf}',
                                workspace=workspace, action="created")
        if reg["failed"] or reg["name_taken"] or reg["unconfirmed"]:
            code = 1
    _write(out, "generated_jobs.json", payload)
    _write(out, "GENERATED_JOBS.md", render_generated_jobs(payload, plan))
    s = payload["summary"]
    print(f'  jobs: {s["refresh_jobs"]} refresh job(s) for table snapshots, '
          f'{s["task_jobs"]} task-graph job(s) ({s["tasks_translated"]} '
          f'translated, {s["task_stubs"]} stub(s)); '
          f'{s["refresh_not_generated"]} refresh(es) not generated')
    if s.get("tasks_note"):
        print(f'  jobs: {s["tasks_note"]}', file=sys.stderr)
    if not payload["registered"]:
        print("  jobs: nothing registered -- offline. `jobs --register` "
              "creates them in AIDP, unscheduled")
        return code
    reg = payload["registration"]
    print(f'  jobs: {len(reg["created"])} created in {reg["folder"]}; '
          f'schedule {reg["schedule"]}')
    for name in reg["name_taken"]:
        print(f"  NOT registered: {name} already exists and was not adopted",
              file=sys.stderr)
    for name in reg["failed"] + reg["unconfirmed"]:
        print(f"  NOT confirmed: {name} (see GENERATED_JOBS.md)",
              file=sys.stderr)
    return code


def _windows(values) -> list[tuple[str, str]]:
    out = []
    for v in values or []:
        start, sep, end = str(v).partition("/")
        if not sep or not start or not end:
            raise ValueError(f"--exclude-window wants START/END ISO times, "
                             f"got {v!r}")
        out.append((start, end))
    return out


def cmd_tokens(args) -> int:
    """LLM token usage per stage and phase. Reads local files only."""
    out = pathlib.Path(args.out_dir)
    rep = build_token_report(
        out, transcripts=[pathlib.Path(t) for t in args.transcript] or None,
        since=args.since, exclude=_windows(args.exclude_window))
    _write(out, "tokens.json", rep)
    _write(out, "TOKENS.md", render_tokens(rep))
    if not rep.get("measured"):
        print(f'  tokens: not measured -- {rep["reason"]}', file=sys.stderr)
        return 0
    for stage, b in rep["by_stage"].items():
        if b.get("measured") is False:
            print(f'  {stage:<14} {"not measured":>12}')
            continue
        print(f'  {stage:<14} {b["total"]:>12,} tokens  ({b["messages"]} call(s))')
    for phase, b in rep["by_phase"].items():
        if b.get("measured") is False:
            print(f'  phase {phase:<10} {"not measured":>10}')
            continue
        print(f'  phase {phase:<10} {b["total"]:>10,} tokens')
    print(f'  TOTAL          {rep["totals"]["total"]:>12,} tokens'
          + (f'  (partial: {rep["unmeasured_runs"]} stage run(s) not '
             f'measured)' if rep.get("partial") else ""))
    return 0


def cmd_data_options(args) -> int:
    out = pathlib.Path(args.out_dir)
    options = options_for(args.phase) if args.phase else list(DATA_OPTIONS)
    # `implemented` is about the OPTIONS listed: each is a proposal. The note
    # must not say more than that. It used to say this plugin "moves no bytes
    # and implements no transfer path", beside a DATA_MOVEMENT_OPTIONS.md
    # from the same run naming the transfer path that does exist. The note is
    # the markdown's own opening sentence, so the two cannot drift again.
    payload = {"options": options, "implemented": False,
               "note": DATA_OPTIONS_NOTE}
    if args.choose:
        if not args.rationale:
            raise ValueError("--choose requires --rationale")
        custom = None
        if args.custom_name or args.custom_description_file:
            if not (args.custom_name and args.custom_description_file):
                raise ValueError(
                    "a custom architecture needs both --custom-name and "
                    "--custom-description-file")
            custom = {
                "name": args.custom_name,
                "description": pathlib.Path(
                    args.custom_description_file).read_text(encoding="utf-8").strip()}
        payload["choice"] = record_choice(
            args.choose, chosen_by=args.chosen_by, rationale=args.rationale,
            custom_architecture=custom)
        state = ("DEFERRED — the customer will specify it later"
                 if payload["choice"]["deferred"] else args.choose)
        print(f"  recorded: {state} (executed: False)")
    _write(out, "data_options.json", payload)
    _write(out, "DATA_MOVEMENT_OPTIONS.md", render_data_options(options))
    print(f"  {len(options)} option(s) presented, each a proposal; the "
          f"implemented copy is the snowmig_02_copy_schema job")
    return 0


def cmd_init_config(args) -> int:
    """Put a fill-me-in config where the operator works.

    The template ships with the plugin, which may be installed read-only, so
    the copy lands in the working directory by default.
    """
    template = PLUGIN_ROOT / TEMPLATE_NAME
    destination = pathlib.Path(args.path or CONFIG_NAMES[0])
    written = write_template(destination, template=template,
                             overwrite=args.force)
    print(f"  wrote {written}")
    print("  Fill it in — the Snowflake connection and the AIDP destination "
          "both live there.")
    print("  IT WILL HOLD LIVE CREDENTIALS: keep it out of git, tickets and "
          "chat.")
    print(f"  Then: snowmig.py preflight --out-dir ./snowmig_out "
          f"--config {written} --test-source")
    return 0


def cmd_preflight(args) -> int:
    """Echo the connection config back, then test both ends.

    The echo is the point: a config nobody read is where the expensive
    failures come from. Testing is best-effort on whichever end was supplied,
    and a skipped end is reported as skipped, never as a pass.
    """
    from plan.preflight import render_preflight_report, run_preflight

    out = pathlib.Path(args.out_dir)
    # Required here: reading the config back is the whole point of the stage.
    _config_path(args, required=True)
    config = snowflake_block(_load_migration_config(args))

    run_sql = None
    if args.test_source:
        run_sql = _run_sql_from_args(args) if args.account else None
        if run_sql is None:
            # Fall back to the config's own coordinates: the whole point is to
            # test what the FILE says, not a second set of arguments.
            # Testing "what the file says" means going through the same
            # resolution every other stage uses, inline secrets included.
            run_sql = _run_sql_from_args(args)

    # The destination half comes from the same file, so preflight checks
    # what the config SAYS rather than a second set of arguments.
    coords = _target_coords(args)
    catalog = coords["catalog"]
    call = None
    if catalog and coords["datalake_ocid"]:
        target = resolve_target(
            datalake_ocid=coords["datalake_ocid"],
            # Only the catalog is read here; the other two are placeholders
            # so a list call can be built without inventing real values.
            workspace=coords["workspace"] or "unused",
            cluster_id=coords["cluster_id"] or "unused",
            catalog=catalog)
        call = make_call(target, backend=args.backend or detect_backend(),
                         run_process=_oci_runner(args))

    result = run_preflight(config, run_sql=run_sql, call=call,
                           catalog=catalog)
    _write(out, "preflight.json", result)
    report = render_preflight_report(result)
    _write(out, "PREFLIGHT_CONFIG.md", report)
    for line in report.splitlines():
        print(f"  {line}" if line else "")
    return 0 if result["ok"] else 1


def cmd_provision(args) -> int:
    """Provision the migration environment inside AIDP.

    Workspace -> cluster `migration-assets` -> (libraries) -> the
    `backup-snowflake-migration/` folder with the data-migration scripts and
    the plan artifacts -> four parametrised jobs. Dry run unless --execute.
    The EXTERNAL catalog is registered by the `catalog` stage, not here.
    """
    from target.provisioning import (
        carry_forward, make_provision_call, provision, render_provision)

    _refuse_by_decision(args, creating=True,
                        warehouse_clusters=bool(args.warehouse_clusters))
    out = pathlib.Path(args.out_dir)
    # The stage NOTEBOOKS are built from the canonical sources at call time,
    # so nothing has to be kept in sync by hand. `--scripts-dir` still
    # overrides where the canonical sources are read from.
    scripts_dir = (pathlib.Path(args.scripts_dir) if args.scripts_dir else
                   dataplane_dir())
    missing = [st.source for st in STAGES
               if not (scripts_dir / st.source).is_file()]
    if missing:
        raise ValueError(
            f"stage source(s) missing from {scripts_dir}: "
            f"{', '.join(missing)}. The engine is the method -- a missing "
            f"stage is a broken install, not a reason to improvise one.")
    scripts = [scripts_dir / st.source for st in STAGES]

    # Whatever plan artifacts exist travel with the scripts, so the migration
    # plan lives NEXT TO the runs it drives, inside AIDP.
    # Runbook S11: one copy workflow per schema of the APPROVED plan -- the
    # ddl_plan.json this push places in plan/. No plan yet, no copy jobs:
    # the schemas are read from the plan, never typed by hand. The demo
    # reads its inputs through the same helper.
    from target.provisioning import plan_push_inputs
    plan_files, copy_schemas = plan_push_inputs(out)

    # In connector mode the in-AIDP scripts need the connection config on the
    # mount. It carries the credential, so it is uploaded ONLY when the user
    # passed it explicitly for this purpose.
    # One AIDP cluster per Snowflake warehouse, named after it. The list
    # comes from the `compute` stage's own artifact, so the names are the
    # ones actually observed in the account -- never typed by hand.
    warehouse_clusters = []
    if args.warehouse_clusters:
        if not (out / "warehouses.json").is_file():
            raise FileNotFoundError(
                "--warehouse-clusters needs warehouses.json; run "
                "`snowmig.py compute` first so the warehouse names come from "
                "the account rather than from memory")
        warehouse_clusters = _read(out, "warehouses.json").get("warehouses", [])
        if args.warehouse:
            wanted = {w.lower() for w in args.warehouse}
            warehouse_clusters = [w for w in warehouse_clusters
                                  if str(w.get("name", "")).lower() in wanted]
            missing = wanted - {str(w.get("name", "")).lower()
                                for w in warehouse_clusters}
            if missing:
                raise ValueError(
                    f"warehouse(s) not in warehouses.json: "
                    f'{", ".join(sorted(missing))}')

    stage_params = {}
    for pair in getattr(args, "stage_param", []) or []:
        if "=" not in pair:
            raise MissingTarget(
                f"--stage-param {pair!r} is not NAME=VALUE. The name is a "
                f"stage parameter as it appears in the notebook's PARAMS "
                f"cell, for example schema=SALES, or with a stage prefix "
                f"to reach that stage only, copy_schema.mode=overwrite.")
        name, value = pair.split("=", 1)
        stage_params[name.strip()] = value.strip()

    source_config = None
    if args.source_config:
        source_config = pathlib.Path(args.source_config)
        if not source_config.is_file():
            raise FileNotFoundError(
                f"--source-config {source_config} not found")
        # Not appended to plan_files: provision() derives the `snowflake:`
        # block and uploads that; the operator's file itself never travels.

    requirements = None
    if not args.skip_libraries:
        requirements = (pathlib.Path(args.requirements) if args.requirements
                        else scripts_dir / "requirements-aidp.txt")

    # The catalogs baked into the jobs' PARAMS defaults may come from the
    # config's `aidp:` block (announced by _aidp_from_config like every
    # other config-derived value); a flag still wins. Read for the dry run
    # too, so PROVISION.md previews the defaults --execute will bake in.
    aidp = _aidp_from_config(args)
    external_catalog = args.external_catalog or aidp.get("external_catalog")
    target_catalog = args.target_catalog or aidp.get("target_catalog")

    # A re-push into THIS migration's own workspace (the plan, after S7/S9)
    # inherits the coordinates the first push baked in, so the notebooks it
    # adds -- the per-schema copy workflows -- carry the same catalogs and
    # the same credential path as the stages already there. provision()
    # decides that from the earlier EXECUTED record: same aiDataPlatform,
    # same workspace key (known only once it is listed), and the inherited
    # credential looked for before it is used; a flag still wins.
    earlier = (_read(out, "provision_result.json")
               if _executed_record_exists(out, "provision_result.json")
               else None)
    ocid = args.datalake_ocid or aidp.get("datalake_ocid")

    call = None
    if args.execute:
        if not ocid:
            raise MissingTarget(
                "--execute needs the aiDataPlatform OCID: put it under "
                "`aidp:` in the config, or pass --datalake-ocid.")
        call = make_provision_call(ocid, run_process=_oci_runner(args))
    elif _executed_record_exists(out, "provision_result.json"):
        return _refuse_dry_run_overwrite(out, "provision_result.json",
                                         "provision")

    # The cluster name: the flag, else -- on a --reuse-existing re-push into
    # the workspace the earlier executed record names, on the same
    # aiDataPlatform -- the name that push used, else the default. Without
    # this, the documented plan push (`--reuse-existing --workspace-name`,
    # no --cluster-name) after an S1 run with --cluster-name looked for
    # `migration_assets`, did not find it, and CREATED a second cluster and
    # bound the jobs to it.
    cluster_name = args.cluster_name
    if not cluster_name and args.reuse_existing and earlier:
        from target.naming import translate_name
        same_ws = ((earlier.get("workspace") or {}).get("name")
                   == translate_name(args.workspace_name,
                                     kind="workspace").name)
        same_lake = (not earlier.get("datalake_ocid") or not ocid
                     or earlier.get("datalake_ocid") == ocid)
        prior_cluster = (earlier.get("cluster") or {})
        if same_ws and same_lake and prior_cluster.get("name"):
            cluster_name = (prior_cluster.get("requested")
                            or prior_cluster["name"])
            print(f"  cluster name taken from provision_result.json: "
                  f"{prior_cluster['name']} (pass --cluster-name to "
                  f"override)")
    res = provision(
        call=call, workspace_name=args.workspace_name,
        cluster_name=cluster_name or "migration-assets",
        scripts=list(scripts),
        stage_params=stage_params,
        plan_files=plan_files, requirements=requirements,
        maven=args.maven or [], external_catalog=external_catalog,
        target_catalog=target_catalog, source_mode=args.source_mode,
        source_config=source_config,
        warehouse_clusters=warehouse_clusters, execute=args.execute,
        subnet_id=args.subnet_id, reuse_existing=args.reuse_existing,
        warehouse_cluster_mode=_compute_mode(args)["warehouse_clusters"],
        existing_cluster_id=_compute_mode(args)["cluster_id"],
        output_dir=_reporting(args)["workspace_dir"],
        refresh_notebooks=args.refresh_notebooks,
        plan_label=args.plan_label, copy_schemas=copy_schemas,
        delete_stale_copy_jobs=getattr(args, "delete_stale_copy_jobs", False),
        prior=earlier, datalake_ocid=ocid)
    if res.get("inherited_from"):
        print(f"  re-push into this migration's workspace "
              f"{res['workspace'].get('name')}: catalogs and credential "
              f"path taken from provision_result.json where no flag gave "
              f"them" + (" (provisional: an executed run confirms the "
                         "workspace key and looks for the credential first)"
                         if res["inherited_from"].get("provisional") else ""))
    # No executed push drops what an earlier one allocated. A re-push finds
    # what the first push created and records it as reused; the earlier
    # executed record is the proof it was created here, and whatever this
    # push does not record itself (the plan push carries no
    # --warehouse-clusters) is kept as `earlier_allocations`, so teardown
    # still reaches it -- and still leaves alone what this migration never
    # created.
    if (not res["dry_run"] and not res["workspace"].get("key") and earlier
            and (earlier.get("workspace") or {}).get("key")):
        # Halted before any key was recorded (name_taken without
        # --reuse-existing): the earlier record is the only one that names
        # what was created, and PROVISION.md is what the halt tells the
        # operator to read. Kept; this run goes beside it.
        _write(out, "provision_result.halted.json", res)
        _write(out, "PROVISION_HALTED.md", render_provision(res))
        print(f"  provision halted before recording a workspace key; the "
              f"earlier executed record (workspace "
              f"{earlier['workspace'].get('name')}) was kept in "
              f"provision_result.json and PROVISION.md, and this run was "
              f"written to provision_result.halted.json / "
              f"PROVISION_HALTED.md", file=sys.stderr)
        return 1
    carry_forward(res, earlier)
    _write(out, "provision_result.json", res)
    _write(out, "PROVISION.md", render_provision(res))

    for obj in res.get("credential_objects") or []:
        # Said out loud, dry run or not: this is the one object this plugin
        # places anywhere that holds a secret. Executed, the list holds only
        # what was read back on the workspace.
        print(f"  CREDENTIAL ON THE WORKSPACE MOUNT: {obj} "
              f"{'would hold' if res['dry_run'] else 'holds'} the Snowflake "
              f"connection block, credential included -- readable by every "
              f"member of workspace {res['workspace']['name']} and every "
              f"cluster in it via /Workspace. Remove it when the migration "
              f"is done.", file=sys.stderr)
    for obj in res.get("credential_unconfirmed") or []:
        print(f"  CREDENTIAL MAY BE ON THE WORKSPACE MOUNT: {obj} -- its "
              f"upload could not be read back. Check workspace "
              f"{res['workspace']['name']} and remove it if it is there.",
              file=sys.stderr)

    failed = [s for s in res["steps"] if s["verified"] is False]
    if res["dry_run"]:
        print("  provision: dry run — nothing created; see PROVISION.md")
    else:
        print(f'  provision: {len(res["steps"])} step(s), '
              f'{len(failed)} failed/unverified')
        ws_key = (res.get("workspace") or {}).get("key")
        cl_key = (res.get("cluster") or {}).get("key")
        if ws_key and cl_key:
            # The keys every later command needs; the CLI printed neither.
            print(f"  hand-off: --workspace {ws_key} --cluster-id {cl_key} "
                  f"(or aidp.workspace / aidp.cluster_id in the config)")
    return 1 if failed else 0


def cmd_demo(args) -> int:
    """Dev mode. The production pipeline against the built-in emulation.

    Real code, fake transports: the artifacts written are in exactly the
    formats a production run produces, and DEMO.md narrates each stage. The
    out-dir is marked emulated so nothing here can be mistaken for a customer
    run.
    """
    from emulation.runbook import run_demo, run_enterprise_demo
    # The demo gets its own default out-dir: emulated artifacts sitting next
    # to a real run's is exactly the confusion the marker file exists to
    # prevent. (set_defaults on the subparser cannot override the parent
    # parser's already-applied default, so it is resolved here.)
    out = pathlib.Path(_demo_dirs()[0]
                       if args.out_dir == str(default_out_dir())
                       else args.out_dir)
    prepare_out_dir(out)
    enterprise = getattr(args, "estate", "standard") == "enterprise"
    result = run_enterprise_demo(out) if enterprise else run_demo(out)
    print("  DEV MODE — everything below is EMULATED; nothing real was touched")
    for i, line in enumerate(result["narrative"], 1):
        print(f"  {i:>2}. {line}")
    print(f'  -> artifacts in {result["out_dir"]} · start with DEMO.md')
    return 0


def _add_target_args(p) -> None:
    p.add_argument("--datalake-ocid")
    p.add_argument("--workspace")
    p.add_argument("--cluster-id")
    p.add_argument("--catalog")
    p.add_argument("--backend", choices=["aidp_cli", "oci_raw"],
                   help="override backend detection")


def _add_snowflake_args(p) -> None:
    # The config file is the documented single source of coordinates; the
    # flags below stay as an override for one-off runs. Without this, a user
    # who filled the config in would still have to repeat every coordinate on
    # the command line -- which is what the config exists to avoid.
    p.add_argument("--config", "--connection-config", dest="config",
                   help="the ONE migration config (see "
                        "snowmig-config.example.yaml): the Snowflake "
                        "connection and, optionally, the AIDP destination. "
                        "Any flag below overrides what it says")
    p.add_argument("--account")
    p.add_argument("--user")
    p.add_argument("--role")
    p.add_argument("--only-primary-role", action="store_true",
                   help="drop SECONDARY roles for the session, so the run "
                        "sees exactly what --role can see and nothing more. "
                        "Snowflake activates every role granted to the user "
                        "by default, so without this a count attributed to a "
                        "restricted role may have been served by "
                        "ACCOUNTADMIN -- which is the difference between "
                        "rehearsing a least-privilege migration and only "
                        "appearing to")
    p.add_argument("--warehouse")
    p.add_argument("--auth", default=None,
                   choices=["keypair", "pat", "password", "externalbrowser"],
                   help="default: the config's `auth:`, else keypair")
    p.add_argument("--key-path")
    # No --key-passphrase: every secret is a path or lives in the config.
    # main() refuses the old spelling by name (see _REMOVED_SECRET_FLAGS).
    p.add_argument("--pat-path")
    p.add_argument("--password-path")


def _out_dir_parent(default) -> argparse.ArgumentParser:
    """A parent parser carrying `--out-dir`, with the default the caller asks
    for. Built twice: see `build_parser`."""
    parent = argparse.ArgumentParser(add_help=False)
    # ONE artifact directory, and it explains itself. `snowmig_out` was the
    # right idea with the wrong presentation: an unexplained directory of
    # JSON appearing beside the plugin reads as a bug rather than as output.
    #
    # It persists between commands on purpose -- the stages chain, and `plan`
    # reads the `inventory.json` that `assess` wrote -- so it cannot be
    # temporary scratch. What it CAN be is obvious: a name that says what it
    # holds, a README inside it, and a permanent ignore rule.
    parent.add_argument(
        "--out-dir", default=default,
        help=f"where run artifacts go. Default: ./{ARTIFACTS_DIRNAME}/ in "
             f"the working directory — one clearly-named directory that "
             f"explains itself in a README, ignores itself in git, and is "
             f"removed by `snowmig clean`")
    return parent


def build_parser() -> argparse.ArgumentParser:
    # --out-dir is accepted on BOTH sides of the subcommand: after it, which
    # is how every caller writes it (`snowmig plan --out-dir ...`), and
    # before it, which is what the top-level usage line advertises. The two
    # copies need different defaults: argparse applies the chosen
    # subparser's defaults over the root namespace, so a `None` default on
    # the subparser copy silently discarded a root-level value -- and
    # `snowmig --out-dir X clean` then deleted the default directory the
    # operator had not named. SUPPRESS leaves the root value alone.
    common = _out_dir_parent(argparse.SUPPRESS)

    ap = argparse.ArgumentParser(prog="snowmig", description=__doc__,
                                 parents=[_out_dir_parent(None)])
    sub = ap.add_subparsers(dest="cmd", required=True)

    st = sub.add_parser("stages", parents=[common],
                       help="what runs, what has run, what it found (offline)")
    st.add_argument("--write-diagram", action="store_true",
                    help="refresh the phase diagram embedded in ARCHITECTURE.md "
                         "from the stage list")
    st.set_defaults(func=cmd_stages)

    dm = sub.add_parser(
        "demo", parents=[common],
        help="DEV MODE: the whole pipeline against an emulated Snowflake "
             "estate and an emulated AIDP -- no credentials, no network, "
             "nothing real is touched. Writes every real artifact plus "
             "DEMO.md")
    dm.add_argument("--estate", choices=["standard", "enterprise"],
                    default="standard",
                    help="standard (default): SNOWDEMO, the whole pipeline "
                         "down to an emulated deploy. enterprise: SNOWENT -- "
                         "external/Iceberg/hybrid/event tables, shares, "
                         "policies, containers, replication -- through plan, "
                         "ddl, external-registration, share-plan and "
                         "summary, with no AIDP step at all")
    dm.set_defaults(func=cmd_demo)

    a = sub.add_parser("assess", parents=[common],
                       help="read-only estate inventory")
    _add_snowflake_args(a)
    a.add_argument("--database", action="append",
                   help="repeatable; omit to scan all non-system databases")
    a.add_argument("--row-counts", choices=list(ROW_COUNT_MODES),
                   default="metadata",
                   help="metadata (default): Snowflake's maintained count, free "
                        "to read. exact: COUNT(*) per object -- accurate, but it "
                        "EXECUTES every view and costs warehouse time. none: skip")
    a.add_argument("--mapping-defaults", choices=("on", "off"), default=None,
                   help="override mapping.enabled for this run: off restores "
                        "the strict modes (VARIANT blocks, TIMESTAMP_NTZ "
                        "preserved); an explicit --semi-structured / "
                        "--timestamp-ntz still wins")
    a.add_argument("--semi-structured", choices=list(SEMI_STRUCTURED_MODES),
                   default=None,
                   help="string (default, or `mapping.semi_structured` in the "
                        "config): carry VARIANT/OBJECT/ARRAY as JSON text, "
                        "with a warning on every affected column. block: "
                        "block their table pending a typed design")
    a.add_argument("--timestamp-ntz", choices=list(TIMESTAMP_NTZ_MODES),
                   default=None,
                   help="timestamp (default, or `mapping.timestamp_ntz` in "
                        "the config): downgrade Snowflake TIMESTAMP_NTZ to "
                        "Spark TIMESTAMP -- required by the AIDP metastore, "
                        "and it changes timezone semantics. preserve: keep "
                        "TIMESTAMP_NTZ, which `ddl` then halts on")
    a.add_argument("--no-census", action="store_true",
                   help="skip the census of procedures, UDFs, tasks, streams, "
                        "stages, pipes, sequences and file formats. The "
                        "coverage claim then says the estate was not examined")
    a.add_argument("--capture-definitions", action="store_true",
                   help="also capture procedure/UDF bodies, task bodies "
                        "and dynamic-table queries into the census artifact "
                        "(they may contain literals). `snowmig jobs` "
                        "generates task and refresh jobs from them; a "
                        "materialized view's query is always captured with "
                        "the view text")
    a.add_argument("--geospatial", choices=list(GEOSPATIAL_MODES),
                   default=None,
                   help="block (default, or `mapping.geospatial` in the "
                        "config): GEOGRAPHY/GEOMETRY block their table. "
                        "string: carry as GeoJSON text; wkt: carry as WKT "
                        "text (ST_ASWKT). Either way there is no spatial "
                        "type on the target")
    a.set_defaults(func=cmd_assess)

    db = sub.add_parser(
        "databases", parents=[common],
        help="list the source databases this role can see, and which are "
             "migratable (runbook S3 -- the user picks ONE)")
    _add_snowflake_args(db)
    db.set_defaults(func=cmd_databases)

    cl = sub.add_parser(
        "catalogs", parents=[common],
        help="list the catalogs on the target DataLake with the types the "
             "SERVER reports (read-only)")
    _add_target_args(cl)
    cl.set_defaults(func=cmd_catalogs)

    cln = sub.add_parser(
        "clean", parents=[common],
        help=f"delete ./{ARTIFACTS_DIRNAME}/ and the demo output "
                  f"(offline). Refuses to touch an --out-dir you chose "
                  f"yourself")
    cln.set_defaults(func=cmd_clean)

    bn = sub.add_parser(
        "build-notebooks", parents=[common],
        help="regenerate the data-plane stage notebooks from "
             "engine/dataplane/ (offline)")
    bn.add_argument("--scripts-dir",
                    help="where to read the canonical stage sources from "
                         "(default: the plugin's engine/dataplane/)")
    bn.add_argument("--dest",
                    help="where to write the .ipynb "
                         "(default: the plugin's data-migration-scripts/)")
    bn.set_defaults(func=cmd_build_notebooks)

    d = sub.add_parser("deps", parents=[common], help="dependency edges")
    _add_snowflake_args(d)
    d.set_defaults(func=cmd_deps)
    ing = sub.add_parser("ingest", parents=[common],
                         help="turn the in-AIDP discovery manifest into "
                              "inventory.json, so the planning stages can "
                              "read what S6 discovered (runbook S7)")
    ing.add_argument("--manifest", required=True,
                     help="path to discovery_manifest.json, downloaded from "
                          "backup-snowflake-migration/reports/")
    ing.add_argument("--database-name", required=True,
                     help="the Snowflake database the manifest describes. "
                          "Required and never guessed: a manifest does not "
                          "record it, and a wrong name aims the plan at the "
                          "wrong catalog")
    ing.add_argument("--config", "--connection-config", dest="config",
                     help="the migration config; only its `mapping:` block is "
                          "read here -- ingest is offline")
    ing.add_argument("--mapping-defaults", choices=("on", "off"), default=None,
                     help="same meaning as on `assess`")
    ing.add_argument("--semi-structured", choices=list(SEMI_STRUCTURED_MODES),
                     default=None,
                     help="same meaning and default as on `assess`")
    ing.add_argument("--geospatial", choices=list(GEOSPATIAL_MODES),
                     default=None,
                     help="same meaning and default as on `assess`")
    ing.add_argument("--timestamp-ntz", choices=list(TIMESTAMP_NTZ_MODES),
                     default=None,
                     help="same meaning and default as on `assess`")
    ing.set_defaults(func=cmd_ingest)


    mt = sub.add_parser("maintenance", parents=[common],
                        help="maintenance/layout state (needs Snowflake)")
    _add_snowflake_args(mt)
    mt.add_argument("--history-days", type=int, default=30,
                    help="ACCOUNT_USAGE window for reclustering credits and "
                         "DML churn (default 30)")
    mt.add_argument("--probe-table-parameters", action="store_true",
                    help="one SHOW PARAMETERS per table. Exact, but thousands "
                         "of round trips on a real estate; off by default, "
                         "where the level is inferred from effective values")
    mt.set_defaults(func=cmd_maintenance)

    se = sub.add_parser("security", parents=[common],
                        help="masking/row-access policies, secure views, grants")
    _add_snowflake_args(se)
    se.add_argument("--no-grants", action="store_true",
                    help="skip the grant summary (needs ACCOUNT_USAGE)")
    se.set_defaults(func=cmd_security)

    er = sub.add_parser(
        "external-registration", parents=[common],
        help="external and Iceberg tables planned register_in_place: a "
             "generated AIDP registration over OCI Object Storage (needs "
             "Snowflake, read-only; executes nothing, moves no bytes)")
    _add_snowflake_args(er)
    er.set_defaults(func=cmd_external_registration)

    sp = sub.add_parser(
        "share-plan", parents=[common],
        help="outbound shares -> an AIDP Delta Sharing plan (needs "
             "Snowflake, read-only; executes nothing)")
    _add_snowflake_args(sp)
    sp.set_defaults(func=cmd_share_plan)

    p = sub.add_parser("plan", parents=[common], help="waves + medallion layout (offline)")
    p.add_argument("--restrictions",
                   help="JSON file of user restrictions (exclude_databases, "
                        "max_rows, exclude_name_patterns, ...)")
    p.add_argument("--bronze-schema-style", choices=list(SCHEMA_STYLES),
                   default="db_schema",
                   help="with --bronze-catalog-prefix: db_schema (default) "
                        "names the target schema DB_SCHEMA so two same-named "
                        "schemas cannot merge; db names it after the Snowflake "
                        "database alone, giving <prefix>.<database>.<table>")
    p.add_argument("--secure-views", choices=list(SECURE_VIEW_MODES),
                   default="refuse",
                   help="refuse (default): a Snowflake SECURE view cannot "
                        "migrate. as-view: plan it as a PLAIN view -- its "
                        "definition becomes visible and role-based row "
                        "filtering does not carry; every report says so")
    p.add_argument("--bronze-catalog-prefix",
                   help="use ONE bronze catalog with this name instead of "
                        "catalog-per-database (default: mirror the source)")
    p.set_defaults(func=cmd_plan)

    g = sub.add_parser("ddl", parents=[common], help="generate target DDL (offline)")
    g.add_argument("--timestamp-ntz", choices=list(TIMESTAMP_NTZ_MODES),
                   default=None,
                   help="re-map Snowflake TIMESTAMP_NTZ offline, from "
                        "inventory.json, without re-reading Snowflake. "
                        "timestamp: downgrade to Spark TIMESTAMP (the only "
                        "form the AIDP metastore accepts) and record the "
                        "timezone caveat on every affected column. Default: "
                        "keep whatever `assess`/`ingest` recorded")
    g.set_defaults(func=cmd_ddl)

    dep = sub.add_parser("deploy", parents=[common], help="dry-run by default")
    dep.add_argument("--execute", action="store_true")
    dep.add_argument("--no-diagnose", action="store_true",
                     help="skip the one-per-schema probe that tells whether "
                          "a failed create is specific to its object name or "
                          "to the request. The probe writes (and cleans up), "
                          "so it can be turned off")
    dep.add_argument("--transport", choices=["catalog_api", "sql"],
                     default="catalog_api",
                     help="catalog_api (default): create schemas/tables/views "
                          "through the catalog CRUD API. Needs no Spark "
                          "cluster. sql: the SQL-statement path (POST "
                          ".../sql/execute), for a deployment that exposes "
                          "that endpoint")
    _add_target_args(dep)
    dep.add_argument("--chunk-size", type=int, default=25)
    dep.set_defaults(func=cmd_deploy)

    ic = sub.add_parser(
        "init-config", parents=[common],
        help="write a fill-me-in migration config into the current directory "
             "(the one file the whole migration reads)")
    ic.add_argument("--path",
                    help=f"where to write it (default ./{CONFIG_NAMES[0]})")
    ic.add_argument("--force", action="store_true",
                    help="overwrite an existing config — it holds "
                         "credentials, so this is never the default")
    ic.set_defaults(func=cmd_init_config)

    pf = sub.add_parser(
        "preflight", parents=[common],
        help="read the connection config back to the user and test it, "
             "before anything else runs")
    pf.add_argument("--test-source", action="store_true",
                    help="also connect to Snowflake with what the config "
                         "says and report the identity it gets")
    _add_snowflake_args(pf)
    _add_target_args(pf)
    pf.set_defaults(func=cmd_preflight)

    pv = sub.add_parser(
        "provision", parents=[common],
        help="provision the AIDP migration environment: workspace, "
             "migration-assets cluster, cluster libraries, the "
             "backup-snowflake-migration/ folder with the data-migration "
             "scripts and plan artifacts, and the migration jobs: discover, "
             "structure and reconcile, plus one copy job per schema of the "
             "approved plan (each passing `schema` as a task parameter, "
             "read in the notebook with oidlUtils.parameters.getParameter). "
             "Dry-run without --execute")
    pv.add_argument("--config", "--connection-config", dest="config",
                    help="the migration config; its `decisions:` and "
                         "`compute:` blocks are held here")
    pv.add_argument("--workspace-name", required=True,
                    help="the workspace to ensure. The name is translated to "
                         "the simplest safe charset ([a-z0-9_], starts with a "
                         "letter) so it cannot be rejected mid-provisioning; "
                         "the translation is reported")
    pv.add_argument("--cluster-name", default=None,
                    help="the migration cluster's name (default: "
                         "migration-assets). A --reuse-existing re-push into "
                         "the same workspace keeps the name the earlier push "
                         "recorded")
    pv.add_argument("--warehouse-clusters", action="store_true",
                    help="also create ONE compute cluster per Snowflake "
                         "warehouse, named after it, on the AIDP default "
                         "config. Reads the names from warehouses.json (run "
                         "`compute` first). Sizing is NOT carried over — "
                         "COMPUTE_PROPOSAL.md keeps that a decision")
    pv.add_argument("--warehouse", action="append",
                    help="repeatable; with --warehouse-clusters, mirror only "
                         "these warehouses instead of all of them")
    pv.add_argument("--scripts-dir",
                    help="where the canonical stage sources are read "
                         "from (default: the plugin's engine/dataplane/)")
    pv.add_argument("--requirements",
                    help="cluster libraries file (default: requirements-"
                         "aidp.txt next to the scripts; all-comments means "
                         "no libraries — the external-catalog path needs "
                         "none)")
    pv.add_argument("--skip-libraries", action="store_true")
    pv.add_argument("--maven", action="append",
                    help="repeatable Maven coordinates for the Spark "
                         "Snowflake connector fallback")
    pv.add_argument("--source-mode", choices=["connector",
                                              "external-catalog"],
                    default=None,
                    help="how the in-AIDP scripts READ Snowflake. connector "
                         "(default) reads it directly from the cluster and "
                         "needs no catalog crawl; external-catalog uses "
                         "three-part names and needs a "
                         "successful crawl. A --reuse-existing re-push keeps "
                         "the mode the earlier push recorded")
    pv.add_argument("--source-config",
                    help="the Snowflake connection config to place on the "
                         "workspace mount for connector mode. It carries the "
                         "credential, so it is uploaded only when passed")
    pv.add_argument("--external-catalog",
                    help="the registered EXTERNAL catalog name, baked into "
                         "the jobs' default parameters")
    pv.add_argument("--target-catalog",
                    help="the INTERNAL target catalog, baked into the jobs' "
                         "default parameters")
    pv.add_argument("--datalake-ocid",
                    help="the aiDataPlatform OCID; required with --execute")
    pv.add_argument("--subnet-id",
                    help="optional network configuration for a NEW workspace")
    pv.add_argument("--execute", action="store_true")
    pv.add_argument("--reuse-existing", action="store_true",
                    help="adopt a workspace, cluster or job that already "
                         "carries the name instead of stopping. OFF by "
                         "default: a migration creates its own environment "
                         "so its blast radius is knowable, and a taken name "
                         "is a collision to resolve, not a shortcut")
    pv.add_argument("--stage-param", action="append", default=[],
                    metavar="NAME=VALUE",
                    help="a value to write into every stage notebook's "
                         "PARAMS cell that declares it, repeatable. PARAMS "
                         "holds the DEFAULTS: a job task's parameters, read "
                         "with oidlUtils.parameters.getParameter, win over "
                         "them by the same name -- which is how each "
                         "per-schema copy job passes its `schema`, so "
                         "`schema` or copy_schema.schema is refused next to "
                         "those jobs, and so is a `tables` that would narrow "
                         "all of them. NAME is the stage flag without "
                         "`--` (schema, tables, mode, dry-run, counts, ...); "
                         "a name no stage declares is refused. Prefix NAME "
                         "with a stage (discover, structure, copy_schema, "
                         "reconcile) to write that stage only, e.g. "
                         "copy_schema.mode=overwrite: an unqualified value a "
                         "declaring stage would reject is refused (`mode` is "
                         "ddl-plan/ctas/manifest in 01 but "
                         "skip-existing/append/overwrite in 02). A switch "
                         "takes true or false; a list flag (tables, schemas) "
                         "takes a comma-separated value. With "
                         "--reuse-existing it needs --refresh-notebooks, "
                         "since a kept notebook is not rewritten. Scope is "
                         "an INPUT -- never edit the stage logic to make it "
                         "cover less")
    pv.add_argument("--refresh-notebooks", action="store_true",
                    help="with --reuse-existing, regenerate the stage "
                         "notebooks from this run's flags even where they "
                         "already exist. OFF by default: a notebook already "
                         "on the workspace is kept, because its PARAMS cell "
                         "(schema, mode, verify) is edited in the console and "
                         "an overwrite would discard that silently")
    pv.add_argument("--delete-stale-copy-jobs", action="store_true",
                    help="delete the copy jobs an earlier plan registered for "
                         "schemas this plan no longer names (and the "
                         "schemaless generic copy job, once per-schema jobs "
                         "exist). Each delete is read back. OFF by default: "
                         "without it they are reported as stale and left "
                         "for the console")
    pv.add_argument("--plan-label", default=None,
                    help="a word appended to the dated backup of plan.json "
                         "and ddl_plan.json in backup-snowflake-migration/"
                         "backup/, e.g. FULL before an S9 scope reduction "
                         "and REDUCED after. Every push is backed up whether "
                         "or not a label is given; the label only says which "
                         "plan the copy is")
    pv.set_defaults(func=cmd_provision)

    rn = sub.add_parser("run", parents=[common],
                        help="run an AIDP job as a WORKFLOW, poll it to a "
                             "terminal state, and save its output as "
                             "evidence (runbook S6, S10)")
    _add_target_args(rn)
    rn.add_argument("--config", "--connection-config", dest="config",
                    help="the ONE migration config: its `aidp:` block "
                         "supplies --datalake-ocid and --workspace (and the "
                         "oci profile). A flag overrides what it says. "
                         "Default: ./snowmig-config.yaml, then the plugin's")
    rn.add_argument("--job", help="job display name, e.g. snowmig_00_discover")
    rn.add_argument("--job-key", help="job key; use when the name is ambiguous")
    rn.add_argument("--param", action="append", metavar="NAME=VALUE",
                    help="refused, with the route that does set the value: "
                         "a run-level parameter does not reach a notebook "
                         "(the stage notebooks read the job TASK's "
                         "parameters), so it would be ignored. Set stage "
                         "values with `provision --stage-param` or on the "
                         "job's task. Scope and mode are INPUTS -- never "
                         "edit a script to change them")
    rn.add_argument("--refresh", action="store_true",
                    help="re-read the run already recorded in "
                         "run_<job>.json from AIDP -- status, then output "
                         "-- and rewrite its record. Submits, cancels and "
                         "resubmits NOTHING. For a record that went stale "
                         "(the poll budget ran out while the job went on)")
    rn.add_argument("--run-key", default=None,
                    help="with --refresh (implied): the run to read, e.g. one "
                         "started from the console, which has no local "
                         "record. Refused if AIDP says it belongs to another "
                         "job")
    rn.add_argument("--poll-seconds", type=float, default=30.0,
                    help="seconds between polls (default: 30)")
    rn.add_argument("--max-polls", type=int, default=40,
                    help="poll budget (default: 40). Running out is reported "
                         "as STILL RUNNING, never as a verdict")
    rn.add_argument("--cold-start-seconds", type=float,
                    default=COLD_START_SECONDS,
                    help="how long to wait for the CLUSTER TO PICK UP the "
                         "task before cancelling the run and resubmitting "
                         f"(default: {COLD_START_SECONDS:.0f}). A run whose "
                         "task has not started within this window (RUNNING, "
                         "task unstarted) is cancelled and resubmitted")
    rn.add_argument("--cold-start-restarts", type=int,
                    default=COLD_START_RESTARTS,
                    help="how many times a run whose task was not picked "
                         "up may be cancelled and resubmitted (default: "
                         f"{COLD_START_RESTARTS}; 0 disables). Every restart "
                         "is named in the run report; when all are spent the "
                         "last run is cancelled and the stage exits 1 as "
                         "COLD START")
    rn.set_defaults(func=cmd_run)

    cat = sub.add_parser("catalog", parents=[common],
                         help="register the target catalog (EXTERNAL/SNOWFLAKE "
                              "by default); dry-run without --execute")
    _add_target_args(cat)
    cat.add_argument("--catalog-type", choices=["external", "standard"],
                     default="external",
                     help="external (default): register a read-only pointer at "
                          "the live Snowflake source, copying nothing. standard "
                          "(runbook S4): create the managed target catalog as a "
                          "CONTAINER -- its schemas and tables are created on "
                          "AIDP compute by the structure workflow (S10), never "
                          "through the control-plane CRUD API")
    cat.add_argument("--source-type", default="snowflake",
                     help="source type for an EXTERNAL catalog (default: "
                          "snowflake)")
    cat.add_argument("--config", "--connection-config", dest="config",
                     help="the ONE migration config: the Snowflake "
                          "connection to register, and optionally the AIDP "
                          "destination. See snowmig-config.example.yaml")
    cat.add_argument("--description", help="catalog description")
    cat.add_argument("--test-connection", action="store_true",
                     help="after the dry-run/registration, POST the "
                          "documented testConnection action with the "
                          "connection details and poll the async result; "
                          "PENDING is reported as pending, never as pass")
    cat.add_argument("--reuse-existing", action="store_true",
                     help="reuse a catalog of this name and type that this "
                          "migration did not create. Without it, only a "
                          "catalog the resource ledger records this "
                          "migration creating is reused; a catalog of the "
                          "other type is refused either way")
    cat.add_argument("--execute", action="store_true")
    cat.set_defaults(func=cmd_catalog)

    c = sub.add_parser("compute", parents=[common],
                       help="warehouse -> AIDP cluster proposal")
    _add_snowflake_args(c)
    c.add_argument("--credit-price", type=float,
                   help="USD per Snowflake credit; required for a cost model, "
                        "never assumed")
    c.set_defaults(func=cmd_compute)

    sm = sub.add_parser("smoke", parents=[common],
                        help="connectivity + permission check on both ends")
    _add_snowflake_args(sm)
    _add_target_args(sm)
    sm.add_argument("--database", action="append",
                    help="database to probe INFORMATION_SCHEMA in; auto-picked "
                         "from the first non-system database otherwise")
    sm.add_argument("--write-probe", action="store_true",
                    help="prove destination WRITE by creating one uniquely-named "
                         "probe schema and removing it again; if cleanup fails, "
                         "the report names what was left. Skipped with a note "
                         "when the target catalog is EXTERNAL, which is "
                         "read-only by design. A dry run without --execute")
    sm.add_argument("--execute", action="store_true",
                    help="actually run --write-probe; without it the probe is "
                         "a dry run that prints what it would create")
    sm.set_defaults(func=cmd_smoke)

    nb = sub.add_parser("notebook", parents=[common],
                        help="generate the shallow-clone notebook (offline). "
                             "--upload is a dry run without --execute, and "
                             "refused with it; the structure is created by "
                             "`provision` + `run --job snowmig_01_structure` "
                             "(S10)")
    _add_target_args(nb)
    nb.add_argument("--upload", action="store_true",
                    help="say where the notebook would be placed in the AIDP "
                         "workspace (dry run). With --execute the upload is "
                         "refused")
    nb.add_argument("--execute", action="store_true",
                    help="with --upload: refused; the structure is created "
                         "by `run --job snowmig_01_structure` (S10)")
    nb.add_argument("--dry-run", action="store_true",
                    help="accepted for compatibility: the upload is a dry run "
                         "unless --execute is given")
    nb.set_defaults(func=cmd_notebook)

    su = sub.add_parser("summary", parents=[common],
                        help="migration summary table: rows, risk, status")
    _add_target_args(su)
    su.add_argument("--exclude-window", action="append", default=[],
                    metavar="START/END",
                    help="as for `tokens`: a window of other work in the same "
                         "session, left out of the summary's token totals")
    su.set_defaults(func=cmd_summary)

    fe = sub.add_parser("fetch", parents=[common],
                        help="download a file from the migration workspace "
                             "into the out dir (default: the discovery "
                             "manifest runbook S7 reads). Read-only")
    _add_target_args(fe)
    fe.add_argument("--config", "--connection-config", dest="config",
                    help="the ONE migration config: its `aidp:` block "
                         "supplies --datalake-ocid and --workspace")
    fe.add_argument("--path", default=None,
                    help="workspace-relative path of the file (default: "
                         f"{DISCOVERY_MANIFEST_REMOTE})")
    fe.add_argument("--to", default=None,
                    help="local file name inside the out dir (default: the "
                         "remote file's name)")
    fe.set_defaults(func=cmd_fetch)

    jb = sub.add_parser(
        "jobs", parents=[common],
        help="generate AIDP jobs for what Snowflake refreshed or scheduled: "
             "a refresh notebook per migrated dynamic table / materialized "
             "view, one job per task graph (translated SQL bodies, stubs for "
             "the rest). Offline: writes generated_jobs.json, "
             "GENERATED_JOBS.md and the notebooks, registers nothing")
    _add_target_args(jb)
    jb.add_argument("--config", "--connection-config", dest="config",
                    help="the ONE migration config: its `aidp:` block "
                         "supplies the coordinates --register needs")
    jb.add_argument("--register", action="store_true",
                    help="also create the jobs in AIDP, UNSCHEDULED, through "
                         "the provisioning calls: notebooks uploaded and read "
                         "back, a job never created over a name that exists. "
                         "The schedule is recorded, not applied")
    jb.set_defaults(func=cmd_jobs)

    pb = sub.add_parser("publish", parents=[common],
                        help="copy the finished report (inputs + outputs) "
                             "into the workspace; dry run unless --execute")
    _add_target_args(pb)
    pb.add_argument("--execute", action="store_true")
    pb.set_defaults(func=cmd_publish)

    td = sub.add_parser("teardown", parents=[common],
                        help="terminate the clusters this migration "
                             "allocated (stop by default); with --scope, "
                             "remove its credential or everything it "
                             "created. Dry run unless --execute")
    _add_target_args(td)
    td.add_argument("--config", "--connection-config", dest="config",
                    help="the migration config; `teardown.action` and "
                         "aidp.datalake_ocid are read from it")
    td.add_argument("--action", choices=("stop", "delete"), default=None,
                    help="overrides teardown.action in the config "
                         "(default stop)")
    td.add_argument("--scope", choices=("compute", "credential", "all"),
                    default="compute",
                    help="compute (default): the clusters only, the "
                         "migration's output kept. credential: only the "
                         "Snowflake credential provision --source-config "
                         "placed on the workspace (the copy jobs can no "
                         "longer read Snowflake). all: UNDO the migration -- "
                         "credential, jobs, clusters, the catalogs it "
                         "created and the workspace, each only where the "
                         "record proves this migration created it")
    td.add_argument("--include-data", action="store_true",
                    help="with --scope all, also delete the INTERNAL target "
                         "catalog this migration created, with its tables "
                         "and rows. Without it that catalog is kept")
    td.add_argument("--execute", action="store_true")
    td.set_defaults(func=cmd_teardown)

    tk = sub.add_parser("tokens", parents=[common],
                        help="LLM token usage per stage and phase, read from "
                             "the Claude Code transcript (local files only)")
    tk.add_argument("--transcript", action="append", default=[],
                    help="a transcript .jsonl to read instead of the session "
                         "recorded in run_log.jsonl (repeatable)")
    tk.add_argument("--since",
                    help="ISO timestamp; include tokens from here, so setup "
                         "work before the first stage is credited to it")
    tk.add_argument("--exclude-window", action="append", default=[],
                    metavar="START/END",
                    help="ISO START/END; tokens in this window are not this "
                         "migration's (other work in the same session) and "
                         "are counted as excluded. Repeatable")
    tk.set_defaults(func=cmd_tokens)

    do = sub.add_parser("data-options", parents=[common],
                        help="present the data-movement options (PROPOSAL ONLY)")
    do.add_argument("--phase", choices=["historic", "ongoing"])
    do.add_argument("--choose", help="record the chosen option id; executes nothing")
    do.add_argument("--chosen-by", default="unspecified")
    do.add_argument("--rationale", help="required with --choose")
    do.add_argument("--custom-name",
                    help="name of a customer architecture that is NOT one of the "
                         "listed options; use with --choose A6_CUSTOMER_DEFINED")
    do.add_argument("--custom-description-file",
                    help="file describing that architecture, recorded verbatim")
    do.set_defaults(func=cmd_data_options)
    return ap


#: Where run artifacts go when the operator does not choose. The name says
#: what it holds, so nobody opening the plugin has to guess whether it is
#: output, a cache, or something that was left behind by mistake.
ARTIFACTS_DIRNAME = "migration-artifacts"

_ARTIFACTS_README = """\
# migration-artifacts — output of the Snowflake -> AIDP migrator

**This directory is generated, and it is never committed.** Keep it until
the migration is torn down: `provision_result.json` and `resources.jsonl`
are the record `teardown` works from.

## What is in here

The migrator runs as a pipeline of stages, and each one reads the previous
stage's JSON and writes its own plus a Markdown report. This directory is
that hand-off, which is why it persists between commands rather than being a
temporary scratch space:

    assess  -> inventory.json    -> INVENTORY.md
    deps    -> dependencies.json
    plan    -> plan.json         -> PLANNED_OBJECTS.md
    ddl     -> ddl_plan.json     -> DDL_PLAN.md
    catalog -> catalog_result.json -> CATALOG.md
               (+ catalog_result_<name>.json, CATALOG_<name>.md per catalog)
    ...

The `.json` files are the machine hand-off between stages. The `.md` files
are the deliverable a human reads and approves before anything is created on
AIDP.

## Why it is gitignored, permanently

These files name a real Snowflake estate -- its databases, schemas, tables
and columns. That is customer data by any reasonable reading, and it must
not reach a public repository. This directory carries its own `.gitignore`,
so it stays ignored wherever it lives.

The reports can be regenerated by re-running their stage. The record of what
was created on AIDP cannot: it is written when the object is created.

## Removing it

    bin/snowmig clean

Deletes this directory. It refuses to touch an `--out-dir` you named
yourself.
"""


def plugin_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def default_out_dir() -> pathlib.Path:
    """The artifact directory: one, in the working directory, clearly named.

    It persists between commands ON PURPOSE -- the stages chain, and `plan`
    reads the `inventory.json` that `assess` wrote. It lives with the user's
    project, not in the plugin: an installed plugin sits in a per-version
    directory, and a record kept there would be left behind by an update.
    The name says what it holds, it explains itself in a README, and it
    ignores itself in git.
    """
    return pathlib.Path.cwd() / ARTIFACTS_DIRNAME


def prepare_out_dir(path: str | pathlib.Path) -> pathlib.Path:
    """Create the artifact directory and make it self-explanatory.

    A directory this creates, or finds empty, gets a `.gitignore` of its own
    so its contents cannot be committed from the user's repository. A folder
    that already holds files is left alone (`--out-dir .` must not hide the
    user's own files). The default directory also gets a README saying what
    it is.
    """
    out = pathlib.Path(path)
    if out.exists() and not out.is_dir():
        raise ValueError(
            f"--out-dir {out} is an existing file, not a directory")
    fresh = not out.exists() or not any(out.iterdir())
    out.mkdir(parents=True, exist_ok=True)
    is_default = out.resolve() == default_out_dir().resolve()
    if is_default:
        readme = out / "README.md"
        if not readme.exists():
            readme.write_text(_ARTIFACTS_README, encoding="utf-8")
    if fresh or is_default:
        ignore = out / ".gitignore"
        if not ignore.exists():
            # Ignore everything here, including this rule: the contents name
            # a real estate and none of it belongs in a commit.
            ignore.write_text(
                "# Generated migrator output: never committed.\n"
                "# Contents name a real Snowflake estate.\n"
                "*\n", encoding="utf-8")
    return out


def _utf8_streams() -> None:
    """Reports use → — · and friends. On Windows a piped stdout is cp1252 and
    print() would raise UnicodeEncodeError half-way through a stage; artifacts
    are written with an explicit encoding, so the streams get the same."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            pass


#: Flags that once took a secret VALUE on the command line. They are refused
#: by name before argparse sees them, so the answer names where the secret
#: belongs instead of "unrecognized arguments" -- and never echoes the value.
_REMOVED_SECRET_FLAGS = {
    "--key-passphrase": ("key_passphrase", "key_passphrase_path"),
}


def _refuse_inline_secret(argv: list[str]) -> str | None:
    for arg in argv:
        flag = arg.split("=", 1)[0]
        if flag in _REMOVED_SECRET_FLAGS:
            inline, path_field = _REMOVED_SECRET_FLAGS[flag]
            return (f"{flag} is not accepted: a secret on the command line "
                    f"reaches shell history and the process table. Put it in "
                    f"the migration config instead, as `{inline}:` (inline; "
                    f"the file is gitignored) or `{path_field}:` (a file "
                    f"holding it).")
    return None


def main(argv: list[str] | None = None) -> int:
    _utf8_streams()
    refused = _refuse_inline_secret(
        list(sys.argv[1:] if argv is None else argv))
    if refused:
        print(f"error: {refused}", file=sys.stderr)
        return 1
    args = build_parser().parse_args(argv)
    if getattr(args, "out_dir", None) is None:
        args.out_dir = str(default_out_dir())
    # Say where output goes, every time. An artifact the user cannot find is
    # an artifact they do not have. `clean` is exempt from both the message
    # and the create -- building the directory in order to delete it would
    # be absurd.
    if args.func is not cmd_clean:
        print(f"  artifacts: {args.out_dir}")
    # Every stage is logged with its window so token usage can be attributed
    # to it afterwards. `clean` deletes the log; `tokens` only reads it.
    log_it = args.func not in (cmd_clean, cmd_tokens)
    args._started_at = _now()
    code: int | None = None
    try:
        if args.func is not cmd_clean:
            # Inside the try: an --out-dir that cannot be created (an
            # existing file, a permission) is the operator's input, and it
            # gets the same one-line `error:` as every other bad input.
            prepare_out_dir(args.out_dir)
        # One backoff policy for every network call in this process.
        path = _config_path(args)
        retry.set_policy(retry.RetryPolicy(
            **retry_block(load_config(path) if path else {})))
        code = args.func(args)
        return code
    except (AuthError, MissingTarget, RefusedToExecute, CatalogRefused,
            DeployRefused, ProvisionTransportError, CatalogTransportError,
            JobRunCollision,
            ConnectionConfigError, ConfigError, FileNotFoundError, OSError,
            InvalidRestriction, NoBackendAvailable, BackendError,
            ExecutorBackendError,
            SourceWriteRefused, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        code = 1
        return 1
    finally:
        events = retry.drain_events()
        if log_it:
            stage = _log_name(args)
            record_stage_run(args.out_dir, stage, args._started_at,
                             _now(), code,
                             os.environ.get("CLAUDE_CODE_SESSION_ID"),
                             job=getattr(args, "job", None),
                             retries=len(events))
            _record_retries(args.out_dir, stage, events)
            _publish_stage(args)


if __name__ == "__main__":
    sys.exit(main())
