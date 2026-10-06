"""Plugin manifest and skill/command structure."""
import json
import pathlib

import yaml
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

# The Codex plugin carries `.codex-plugin/plugin.json`, not Claude Code's
# manifest, so the checks that read `.claude-plugin/` do not apply.
claude_only = pytest.mark.skipif(
    not (ROOT / ".claude-plugin").is_dir(),
    reason="Claude Code's manifest (.claude-plugin/) is not part of this plugin")

SKILLS = ["snowflake-migrator-overview", "snowflake-migrator-bootstrap",
          "snowflake-assess-estate", "snowflake-migration-plan",
          "snowflake-medallion-clone", "snowflake-compute-proposal",
          "snowflake-smoke-test", "snowflake-clone-notebook",
          "snowflake-stage-board", "snowflake-migrator-demo",
          "snowflake-provision-environment"]
COMMANDS = ["snowflake-assess", "snowflake-plan", "snowflake-soft-clone",
            "snowflake-compute", "snowflake-smoke", "snowflake-notebook",
            "snowflake-catalog", "snowflake-demo", "snowflake-provision"]


def frontmatter(path: pathlib.Path) -> dict:
    text = path.read_text(encoding="utf-8")
    assert text.startswith("---\n"), f"{path} needs YAML frontmatter"
    block = text.split("---", 2)[1]
    # A real YAML parser, not a line splitter. Claude Code parses this block
    # as YAML; an unquoted description containing ': ' is a mapping error
    # there, and the skill then loads with EMPTY metadata. The old
    # split-on-colon reader accepted exactly that file.
    out = yaml.safe_load(block)
    assert isinstance(out, dict), f"{path}: frontmatter is not a YAML mapping"
    return out


@claude_only
def test_manifest_is_valid_and_keeps_the_name():
    m = json.loads((ROOT / ".claude-plugin/plugin.json").read_text(encoding="utf-8"))
    assert m["name"] == "oracle-ai-data-platform-workbench-snowflake-migrator"
    assert "SCAFFOLD" not in m["description"]
    assert m["version"] and m["license"]


@pytest.mark.parametrize("name", SKILLS)
def test_skill_exists_with_matching_frontmatter(name):
    fm = frontmatter(ROOT / "skills" / name / "SKILL.md")
    assert fm["name"] == name
    assert len(fm["description"]) > 60, "description drives routing; make it specific"
    assert "SCAFFOLD" not in fm["description"]
    assert "Databricks" not in fm["description"]


@pytest.mark.parametrize("name", COMMANDS)
def test_command_exists(name):
    fm = frontmatter(ROOT / "commands" / f"{name}.md")
    assert fm["description"] and "SCAFFOLD" not in fm["description"]


def test_no_databricks_scaffold_survives():
    stale = [p for p in (ROOT / "skills").glob("*") if p.is_dir()
             and p.name not in SKILLS]
    assert stale == [], f"stale skill dirs: {[p.name for p in stale]}"
    assert not (ROOT / "agents").exists() or not list((ROOT / "agents").glob("*.md"))


def test_skills_invoke_the_engine_by_plugin_root():
    """Always addressed from the plugin root, never a relative path.

    Either entry point satisfies that: the launcher `bin/snowmig`, which is
    what the docs now use, or the engine directly. What must never appear is
    a path relative to wherever the user happens to be standing.
    """
    accepted = ("<plugin-root>/bin/snowmig",
                "<plugin-root>/engine/snowmig.py")
    for name in SKILLS:
        text = (ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8")
        if "snowmig" in text:
            assert any(a in text for a in accepted), name


def test_clone_skill_states_the_runtime_coordinate_rule():
    text = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "ask the user" in low
    assert "--execute" in text
    assert "dry" in low


def test_type_mapping_reference_covers_the_blocked_types():
    text = (ROOT / "references/type-mapping.md").read_text(encoding="utf-8")
    for t in ["NUMBER", "TIMESTAMP_NTZ", "VARIANT", "GEOGRAPHY"]:
        assert t in text


def test_reference_documents_the_object_and_view_mapping():
    text = (ROOT / "references/type-mapping.md").read_text(encoding="utf-8")
    # A Snowflake database maps to a catalog, and which KIND of catalog is the
    # part a reader has to get right: EXTERNAL by default, Standard on request.
    assert "EXTERNAL catalog" in text
    assert "Standard catalog" in text
    for construct in ["QUALIFY", "LATERAL FLATTEN", "LISTAGG", "DATEADD"]:
        assert construct in text, construct
    assert "secure view" in text.lower()


def test_plan_skill_documents_the_cannot_migrate_categories():
    """Every category the planner can emit and the report can title must be
    explained in the skill that reads the reasons out. The list once named
    four of them by hand and missed `dependency_not_migrated` the day
    plan/build.py started emitting it."""
    from report.render import _CATEGORY_TITLES
    text = (ROOT / "skills/snowflake-migration-plan/SKILL.md").read_text(encoding="utf-8")
    for category in _CATEGORY_TITLES:
        assert f"`{category}`" in text, category
    assert "restrictions" in text.lower()
    # `unsupported_object` is not only the two view kinds: plan/build.py
    # `_TABLE_KIND_BLOCKS` files three SHOW TABLES flags under it too. The
    # external and Iceberg flags moved to `register_in_place`.
    row = next(l for l in text.splitlines()
               if l.startswith("| `unsupported_object` |"))
    for kind in ("dynamic", "event", "hybrid"):
        assert kind in row, f"unsupported_object also covers {kind} tables"
    row = next(l for l in text.splitlines()
               if l.startswith("| `register_in_place` |"))
    for kind in ("External", "Iceberg", "not copied", "OCI Object Storage"):
        assert kind in row, f"register_in_place row must say {kind}"


def test_clone_skill_documents_one_catalog_per_run_and_cli_backends():
    text = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "one catalog per run" in low
    assert "aidp" in low and "oci" in low
    assert "empty" in low, "must say the cloned objects hold no data"
    assert "silver" in low and "never triggered" in low


def test_no_skill_still_promises_tables_only():
    # Views came into scope; a stale "tables only" line would mislead.
    for name in SKILLS:
        text = (ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8").lower()
        assert "tables only" not in text, name


def test_smoke_skill_documents_the_write_probe_lifecycle():
    """The probe creates one schema, removes it again, and names anything a
    failed cleanup left behind. An earlier test (and the CLI help) still
    described the pre-cleanup behaviour -- "it is NOT dropped afterwards" --
    long after the code was corrected; this one pins the corrected claim."""
    text = (ROOT / "skills/snowflake-smoke-test/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "write-probe" in low
    assert "drop" in low or "remove" in low, "must say it cleans up after itself"
    assert "left behind" in low or "cleanup fails" in low, \
        "must say a failed cleanup is named, not hidden"
    assert "external" in low, \
        "must say the probe is skipped for a read-only EXTERNAL catalog"
    # plan/smoke.py `_probe_schema_name()` suffixes PROBE_SCHEMA with 8 hex
    # chars per run and drops
    # only what this run created -- there is no pre-existence check. The
    # skill once described a constant name and a skipped drop.
    from plan.smoke import PROBE_SCHEMA
    flat = " ".join(low.split())
    for name, doc in (("smoke skill", flat),):
        assert (PROBE_SCHEMA + "_") in doc, \
            f"{name}: must name the per-run suffixed probe schema"
        for stale in ("creates a schema named `snowmig_permission_probe`",
                      "already there", "pre-existed", "constant schema"):
            assert stale not in doc, f"{name}: {stale!r}"


def test_notebook_skill_says_execution_is_the_users_call():
    text = (ROOT / "skills/snowflake-clone-notebook/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "do not run it for them" in low
    assert "empty" in low, "must say the tables arrive with no rows"
    assert "workspace" in low and "not in a data catalog" in low


def test_every_skill_that_can_write_states_the_no_data_guarantee():
    for name in ("snowflake-medallion-clone", "snowflake-clone-notebook"):
        low = (ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8").lower()
        assert "no data" in low or "moves no data" in low or "copies no data" in low, name


def test_dialect_translation_reference_reports_honest_coverage():
    import sys
    sys.path.insert(0, str(ROOT / "engine"))
    from snowflake_source.dialect.translate import coverage

    text = (ROOT / "references/dialect-translation.md").read_text(encoding="utf-8")
    c = coverage()
    assert f"Implemented ({c['implemented']})" in text
    assert f"({c['declared']})" in text
    for rule_id in c["implemented_rule_ids"] + c["declared_rule_ids"]:
        assert rule_id in text, rule_id
    assert "never approximate" in text.lower()


def test_overview_states_the_source_read_only_guarantee_as_enforced():
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "enforced" in low
    assert "ever written to or dropped from the source" in low
    assert "assume none" in low, "no destination means no assumption"


def test_data_movement_reference_offers_at_least_three_options():
    import sys
    sys.path.insert(0, str(ROOT / "engine"))
    from plan.data_movement import OPTIONS

    text = (ROOT / "references/data-movement-options.md").read_text(encoding="utf-8")
    assert len(OPTIONS) >= 3
    for o in OPTIONS:
        assert o["id"] in text, o["id"]
    assert "moves no bytes" in text.lower()


def test_overview_requires_the_options_to_be_presented_always():
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    flat = " ".join(text.lower().split())
    assert "always present the data-movement architecture options" in flat
    assert "undecided" in flat
    assert "none of the six is implemented" in flat


def test_plan_skill_lists_every_option_with_a_stated_recommendation():
    text = (ROOT / "skills/snowflake-migration-plan/SKILL.md").read_text(encoding="utf-8")
    for opt in ("`A1`", "`A2`", "`A3`", "`A4`", "`A5`", "`A6`"):
        assert opt in text, opt
    # Collapse whitespace: markdown line wrapping must not break a prose check.
    flat = " ".join(text.lower().split())
    assert "recommendation, not a decision" in flat
    assert "undecided" in flat
    assert "none of the six is implemented" in flat


def test_reference_carries_the_capability_matrix_and_build_notes():
    text = (ROOT / "references/data-movement-options.md").read_text(encoding="utf-8")
    assert "Capability matrix" in text
    assert "What each option would take to build" in text
    for cap in ("historic_bulk", "ongoing_incremental", "read_without_copy"):
        assert cap in text, cap


def test_skills_present_the_open_slot_as_a_valid_answer():
    for name in ("snowflake-migrator-overview", "snowflake-migration-plan"):
        flat = " ".join((ROOT / "skills" / name / "SKILL.md")
                        .read_text(encoding="utf-8").lower().split())
        assert "a6" in flat, name
        assert "never paraphrase" in flat or "never mapped" in flat, name


def test_no_skill_pushes_the_user_to_pick_from_the_listed_options():
    flat = " ".join((ROOT / "skills/snowflake-migration-plan/SKILL.md")
                    .read_text(encoding="utf-8").lower().split())
    assert "real answer, not a fallback" in flat


def test_no_shipped_file_mentions_the_forked_source_platform():
    """This is a Snowflake migrator. Nothing shipped should say otherwise.

    The plugin began as a copy of a sibling plugin's layout, and three
    inherited files still described it -- PRIVACY.md named the wrong plugin and
    the wrong data flows, NOTICE named the wrong plugin and author, and the
    changelog documented files that do not exist here.

    Test files are exempt: several of them assert the ABSENCE of that scaffold
    and must be able to name what they are excluding.
    """
    root = pathlib.Path(__file__).resolve().parents[2]
    banned = ("databricks", "dbutils", "dbfs")
    # Two OSS Delta Spark settings are spelled with that legacy vendor prefix
    # and OSS honours it, so the name cannot be changed without making the
    # documentation wrong. Only the literal config prefix is exempt -- prose
    # about the other platform is still a failure.
    allowed_literals = ("spark.databricks.delta.",)
    offenders = []
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in (".md", ".py", ".json", ".txt", ".sql"):
            continue
        rel = path.relative_to(root)
        parts = set(rel.parts)
        if "tests" in parts or "__pycache__" in parts or ".pytest_cache" in parts:
            continue
        low = path.read_text(errors="ignore", encoding="utf-8").lower()
        for literal in allowed_literals:
            low = low.replace(literal, "")
        hits = [b for b in banned if b in low]
        if hits:
            offenders.append(f"{rel}: {hits}")
    assert not offenders, "shipped files still reference the forked platform:\n" + "\n".join(offenders)


def test_every_cli_stage_is_invoked_by_at_least_one_skill():
    """No stage may be reachable only by reading the README.

    `summary` produces SUMMARY.md -- the per-object roll-up that is one of the
    plugin's headline deliverables -- and for several versions no skill or
    command mentioned it, so Claude would only have run it by accident. Same
    for `maintenance` the day it was added.
    """
    root = pathlib.Path(__file__).resolve().parents[2]
    cli = (root / "engine" / "snowmig.py").read_text(encoding="utf-8")
    stages = set(re.findall(r'sub\.add_parser\(\s*"([a-z-]+)"', cli))
    assert stages, "no stages parsed -- the regex needs updating"

    invoked: set[str] = set()
    for path in list((root / "skills").rglob("SKILL.md")) + \
            list((root / "commands").glob("*.md")):
        text = path.read_text(encoding="utf-8")
        # Both invocation forms count: `snowmig.py <stage>` and the
        # launcher, `bin/snowmig <stage>`, which is what the docs now use.
        # `snowmig-test` cannot match -- the pattern needs whitespace
        # straight after the name (or the closing quote of a quoted path).
        invoked |= set(re.findall(r"snowmig(?:\.py)?\"?\s+([a-z-]+)", text))

    orphaned = sorted(stages - invoked)
    assert not orphaned, (
        f"these CLI stages are not invoked by any skill or command, so nothing "
        f"will ever run them: {orphaned}")


def test_the_readme_carries_a_runnable_from_zero_runbook():
    """Someone arriving with no context must find the order of operations.

    A fresh conversation has no memory of how the last migration was driven,
    so the sequence has to live in the repo, name the config file, and cover
    every stage that writes."""
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "How to run a migration, from zero" in text
    # The config file is the single place coordinates and secrets live.
    assert "snowmig-config.example.yaml" in text
    # Every stage a migration cannot be run without, in whichever form the
    # runbook writes the invocation (`snowmig.py assess` or `$E assess`).
    for stage in ("init-config", "preflight", "assess", "plan", "ddl",
                  "smoke", "catalog", "provision"):
        # `snowmig.py <stage>`, the launcher `snowmig <stage>`, or $E.
        assert (f"snowmig.py {stage}" in text
                or f"snowmig {stage}" in text
                or f"$E {stage}" in text), stage
    # The four in-AIDP jobs and the deliverable they produce.
    for job in ("snowmig_00_discover", "snowmig_01_structure",
                "snowmig_02_copy_schema", "snowmig_03_reconcile"):
        assert job in text, job
    assert "MIGRATION_REPORT.md" in text


def test_the_runbook_states_the_two_things_it_must_not_let_slide():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    low = text.lower()
    # A target catalog that exists as a CONTAINER only, and a cutover the
    # plugin cannot make consistent on its own.
    #
    # This used to assert the README said the target catalog is "not created
    # by this plugin". That was false: `catalog --catalog-type standard
    # --execute` creates it (runbook S4, live-verified), and the README, the
    # runbook and the CLI disagreed with each other. The invariant that
    # actually matters is the one that misleads if dropped -- the container
    # is not the structure, because a control-plane table create can return
    # 202 Accepted and create nothing.
    assert "container" in low
    assert "202 accepted" in low
    assert "cutover" in low
    assert "freeze writers" in low


def test_the_router_points_at_the_runbook():
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    assert "README.md" in text
    assert "from zero" in text.lower()
    # And names the config file as the first thing to establish.
    assert "snowmig-config.example.yaml" in text


def test_the_docs_say_where_each_credential_lives():
    """"Where do I put the URL, the user and the password?" is the question
    users actually ask, and answering it wrong once costs a leaked secret.

    Two places: the Snowflake coordinates AND the Snowflake secret in the
    one config file (inline is the documented default since 0.19; a `*_path`
    variant is the opt-in), and AIDP auth in the user's own OCI config.
    """
    for path in ("README.md",
                 "skills/snowflake-migrator-bootstrap/SKILL.md",
                 "snowmig-config.example.yaml"):
        text = (ROOT / path).read_text(encoding="utf-8")
        low = text.lower()
        # AIDP authentication is NOT this plugin's business.
        assert "~/.oci/config" in text, f"{path}: AIDP auth is the OCI config"
        # One file holds both ends, and it holds live credentials.
        assert "snowmig-config" in text, f"{path}: name the one config file"
        # And the rule that protects it.
        assert "never" in low and "chat" in low, \
            f"{path}: must say a secret is never asked for in chat"
    # The skill that drives the catalog stage reads that file too.
    low = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text(encoding="utf-8").lower()
    assert "never" in low and "chat" in low, \
        "medallion-clone: must say a secret is never asked for in chat"


def test_no_skill_or_command_claims_the_config_carries_no_secret():
    # The pre-0.19 contract (credential = a path, so the file is safe to
    # read and show) survived in one skill after inline became the default.
    paths = sorted((ROOT / "skills").glob("*/SKILL.md")) + \
        sorted((ROOT / "commands").glob("*.md")) + \
        [ROOT / "README.md", ROOT / "ARCHITECTURE.md"]
    for path in paths:
        flat = " ".join(path.read_text(encoding="utf-8").lower()
                        .replace("*", "").split())
        assert "carries no secret" not in flat, path.name
        assert "credential itself is a path" not in flat, path.name


def test_clone_skill_carries_the_inline_secret_rules():
    low = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text(encoding="utf-8").lower()
    flat = " ".join(low.split())
    assert "inline" in flat
    assert "ask the user before reading" in flat
    assert "never print" in flat or "never quote" in flat
    assert "redact" in flat
    assert "rotate" in flat


def test_no_doc_names_the_nonexistent_aidp_test_connection_verb():
    # `aidp catalog test-connection` was an inferred shape; the wired command
    # is `snowmig catalog ... --execute --test-connection`.
    paths = sorted((ROOT / "skills").glob("*/SKILL.md")) + \
        sorted((ROOT / "commands").glob("*.md")) + \
        [ROOT / "README.md", ROOT / "ARCHITECTURE.md"]
    for path in paths:
        assert "aidp catalog test-connection" not in path.read_text(encoding="utf-8"), path.name
    medallion = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text(encoding="utf-8")
    assert "--test-connection" in medallion


def test_the_docs_do_not_assume_the_user_is_inside_this_repo():
    """An installed plugin has no repo and no open folder: paths come from
    the plugin root, and the config belongs in the working directory."""
    text = (ROOT / "skills/snowflake-migrator-bootstrap/SKILL.md").read_text(encoding="utf-8")
    assert "<plugin-root>/engine/snowmig.py" in text
    assert "init-config" in text, "must say how to create a config from nothing"
    low = text.lower()
    assert "do not assume" in low and "inside this repo" in low
    assert "read-only" in low, \
        "must explain why the config does not live beside the plugin"


def test_the_router_forbids_doing_the_engine_s_work_by_hand():
    """The plugin's value is that a migration is deterministic and audited.

    An agent that cannot find the engine, or that finds a stage inconvenient,
    must not fall back to hand-written SQL and ad-hoc API calls: that leaves
    an estate half-migrated with no artifact saying what happened.
    """
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    low = " ".join(text.lower().split())
    assert "never do by hand what a stage does" in low
    assert "do not re-implement a stage" in low
    assert "do not translate sql or types yourself" in low
    # And the explicit stop condition.
    assert "cannot be found, stop" in low
    assert "<plugin-root>/engine/snowmig.py" in text
    assert "never a reason to improvise" in low


# --- the key files the docs tell the operator to create -----------------------
# `snowmig-config.example.yaml` and the bootstrap skill both generate an
# UNENCRYPTED PKCS#8 key into the working directory (`./migrator_rsa_key.p8`,
# `./sf_key.p8`), and the plugin folder is the documented "convenient spot"
# to work from inside a checkout. `.gitignore` covered `*.pem` and `*.key`
# but not `*.p8`, so a `git add -A` for a doc fix would have staged the key.

KEY_FILES_THE_DOCS_CREATE = ["migrator_rsa_key.p8", "sf_key.p8", "rsa_key.p8",
                             "engine/anything.pk8", "x.pem", "x.key"]


def test_gitignore_names_every_private_key_spelling():
    rules = [line.strip() for line in
             (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()]
    for pattern in ("*.pem", "*.key", "*.p8", "*.pk8", "rsa_key*",
                    "*_rsa_key*", "sf_key*"):
        assert pattern in rules, f".gitignore must carry {pattern!r}"


@pytest.mark.parametrize("name", KEY_FILES_THE_DOCS_CREATE)
def test_gitignore_covers_every_key_file_the_docs_tell_you_to_create(name):
    import shutil
    import subprocess
    if shutil.which("git") is None:
        pytest.skip("git is not on PATH")
    proc = subprocess.run(["git", "check-ignore", "-q", "--", name],
                          cwd=ROOT, capture_output=True, text=True,
                          encoding="utf-8", errors="replace")
    if proc.returncode == 128:
        pytest.skip("the plugin is not inside a git work tree")
    assert proc.returncode == 0, f"{name} would be committed by `git add -A`"


def test_privacy_doc_describes_the_current_credential_and_data_flows():
    """PRIVACY.md is what a security reviewer reads before the live run. It
    described the structure-only era: path-only secrets, no credential in any
    artifact, coordinates not persisted, table data never read, only `deploy`
    writes. The code does the opposite on each point -- the one config file
    holds the credential inline, `provision --source-config` uploads it to
    the workspace, `catalog --execute` sends it to AIDP in connectionDetails,
    the copy stage reads every row -- and an approval obtained on the old text
    would be obtained on false premises."""
    from plan.smoke import PROBE_SCHEMA
    from snowmig import ARTIFACTS_DIRNAME
    text = (ROOT / "PRIVACY.md").read_text(encoding="utf-8")
    flat = " ".join(text.lower().split())
    # What leaves the machine, and how.
    assert "backup-snowflake-migration/plan" in flat, \
        "name the workspace folder the config is uploaded to"
    assert "provision --execute" in flat and "catalog --execute" in flat
    assert "connectiondetails" in flat or "snowflake_password" in flat, \
        "say the credential travels in the catalog registration body"
    assert "raw-request" in flat or "aidp catalog" in flat
    assert "tls" in flat
    # What the data plane does with rows.
    assert "select * from" in flat or "every row" in flat
    # Where things land locally, and what the probe is called.
    assert ARTIFACTS_DIRNAME.lower() in flat
    assert (PROBE_SCHEMA + "_").lower() in flat, \
        "the probe schema carries a per-run suffix"
    assert "pyyaml" in flat
    # `run` starts jobs and has no dry-run gate; say so.
    assert "`run`" in text
    # provisioning.source_config_payload uploads the `snowflake:` block only,
    # as JSON, to plan/<config stem>.json; the operator's file never travels.
    assert "snowmig-config.json" in flat, \
        "name the derived plan/<stem>.json the default config lands as"
    assert "`aidp:` block is not" in flat, \
        "say the aidp: block stays on the laptop"
    # The probe writes only with --execute (snowmig.py cmd_smoke), and
    # `notebook --upload` writes nothing: dry run, refused with --execute.
    assert "write-probe --execute" in flat, \
        "the writers table must show the --execute gate on the probe"
    assert "notebook --upload` is not a writer" in flat
    # The stale claims must be gone, verbatim.
    for stale in ("never accepted as inline",
                  "written into any artifact",
                  "not persisted by the plugin",
                  "table data is never read",
                  "no stage selects rows",
                  "creates a schema named `snowmig_permission_probe`",
                  "leaves nothing behind outside",
                  "uploads that file",
                  "verbatim** to the workspace",
                  "uploaded config under",
                  "| `smoke --write-probe` | opt-in",
                  "| `notebook --upload` | opt-in"):
        assert stale not in flat, f"stale claim still in PRIVACY.md: {stale!r}"
    # And it still says what a reviewer must hear plainly.
    assert "rotate" in flat, "advise rotating the credential after the run"


def test_no_operator_surface_mentions_the_nonexistent_notebook_run_command():
    """`aidp notebook run` is not a command the aidp CLI has, and the
    notebookRuns API does not exist. Neither may be offered to
    an operator as the way to run a notebook."""
    paths = list((ROOT / "skills").rglob("SKILL.md")) \
        + list((ROOT / "commands").glob("*.md")) \
        + [p for p in (ROOT / "engine").rglob("*.py")
           if "tests" not in p.parts]
    offenders = [str(p.relative_to(ROOT)) for p in paths
                 if "aidp notebook run" in p.read_text(encoding="utf-8")]
    assert offenders == []


# --------------------------------------------------------------------------
# "Copies no data" is true of the control plane and false of the plugin: the
# in-AIDP job snowmig_02_copy_schema INSERT-SELECTs every row when the
# operator runs it. Every surface that makes the claim has to scope it.
# --------------------------------------------------------------------------

_NO_DATA_CLAIM = re.compile(
    r"copies no data|moves no bytes|no data is moved|no rows\s+move|"
    r"none implemented|nothing below is implemented|"
    r"no code path can report that data moved", re.I)
_DATA_SURFACES = (".claude-plugin/plugin.json", ".claude-plugin/marketplace.json",
                  ".codex-plugin/plugin.json",
                  "NOTICE", "README.md", "references/data-movement-options.md")


def test_no_surface_claims_the_plugin_copies_no_data_unscoped():
    paths = [ROOT / p for p in _DATA_SURFACES if (ROOT / p).is_file()]
    paths += sorted((ROOT / "skills").glob("*/SKILL.md"))
    offenders = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for para in re.split(r"\n\s*\n", text):
            hit = _NO_DATA_CLAIM.search(para)
            if hit and "02_copy_schema" not in para:
                offenders.append(f"{path.relative_to(ROOT)}: {hit.group(0)!r}")
    assert not offenders, (
        "unscoped 'no data' claims (name snowmig_02_copy_schema in the same "
        "paragraph):\n" + "\n".join(offenders))


@claude_only
def test_manifest_and_marketplace_agree_on_the_data_claim():
    plugin = json.loads((ROOT / ".claude-plugin/plugin.json").read_text(encoding="utf-8"))
    market = json.loads((ROOT / ".claude-plugin/marketplace.json").read_text(encoding="utf-8"))
    entry = market["plugins"][0]
    assert entry["version"] == plugin["version"]
    # Rows move only through the per-schema copy job: both descriptions say
    # so, and neither claims data moves any other way.
    for desc in (plugin["description"], entry["description"]):
        assert "copy job per schema" in desc.lower(), desc


# --------------------------------------------------------------------------
# One order of operations. The overview skill (S1-S12) is the authority and
# the code enforces it: `catalog --execute` needs the workspace and cluster
# that `provision` creates. The README once ran them the other way round and
# bracketed the two coordinates as optional.
# --------------------------------------------------------------------------

def _runbook(text: str) -> str:
    start = text.index("## How to run a migration, from zero")
    end = text.index("\n## ", start + 10)
    return text[start:end]


def test_the_readme_orders_provision_before_catalog_registration():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    runbook = _runbook(text)
    assert "Nine steps" not in text
    assert runbook.index("snowmig provision") < runbook.index("snowmig catalog --catalog")
    provision = runbook.index("Provision the migration environment")
    external = runbook.index("EXTERNAL catalog")
    assert provision < external, "provision (S1/S2) comes before the catalogs (S3/S4)"


def test_the_readme_does_not_bracket_workspace_and_cluster_for_catalog_execute():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    runbook = _runbook(text)
    catalog_step = runbook[runbook.index("EXTERNAL catalog"):]
    catalog_step = catalog_step[:catalog_step.index("\n### ", 10)]
    assert not re.search(r"\[--datalake-ocid <ocid> --workspace <ws> --cluster-id <cl>\]",
                         catalog_step), "required for --execute; the bracket said optional"
    # And the hand-off. The keys live in provision_result.json under
    # workspace.key / cluster.key; PROVISION.md and the CLI output show the
    # display names, which are NOT the keys. The README must send the
    # operator to the file, not to the printout.
    assert "aidp.workspace" in runbook and "aidp.cluster_id" in runbook
    assert "does not write them back" in runbook
    assert "provision_result.json" in runbook
    assert "workspace.key" in runbook and "cluster.key" in runbook
    assert "`PROVISION.md` and the CLI output print" not in runbook, \
        "render_provision prints names, not keys; cmd_provision prints a step count"


def test_every_hand_off_sends_the_operator_to_provision_result_json_for_the_keys():
    # render_provision prints `Workspace: <name>` / `Cluster: <name>` and, on
    # the created path, the display name in the steps table (the key appears
    # there only with --reuse-existing); cmd_provision prints a step count.
    # The keys are recorded in provision_result.json under workspace.key /
    # cluster.key and nowhere else, so every surface that describes the
    # hand-off must point there. A display name pasted as a key addresses
    # nothing that exists.
    arch = (ROOT / "MIGRATION-ARCHITECTURE.md").read_text(encoding="utf-8")
    row = next(l for l in arch.splitlines() if "Provision the AIDP environment" in l)
    assert "provision_result.json" in row, row
    assert "Paste the printed workspace and cluster keys" not in arch
    for rel in ("skills/snowflake-provision-environment/SKILL.md",
                "commands/snowflake-provision.md"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert "provision_result.json" in text, rel
        assert "workspace.key" in text and "cluster.key" in text, rel


def test_the_readme_labels_laptop_assess_as_an_optional_preview():
    runbook = _runbook((ROOT / "README.md").read_text(encoding="utf-8"))
    step = runbook[runbook.index("Assess the estate"):]
    step = step[:step.index("\n### ", 10)]
    low = step.lower()
    assert "optional" in low and "snowmig_00_discover" in step, \
        "the laptop assess is a preview; the migration discovers inside AIDP (S6)"


def test_the_readme_separates_copy_jobs_from_the_migration_proper():
    runbook = _runbook((ROOT / "README.md").read_text(encoding="utf-8"))
    low = " ".join(runbook.lower().split())
    assert "data migration is not run" in low, \
        "must share the overview skill's statement: S12 ends with no rows moved"
    assert runbook.index("snowmig_01_structure") < runbook.index("snowmig_02_copy_schema")


def test_the_three_runbooks_agree_on_the_step_order():
    skill = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    assert skill.index("| S1 |") < skill.index("| S3 |") < skill.index("| S4 |")
    arch = (ROOT / "MIGRATION-ARCHITECTURE.md").read_text(encoding="utf-8")
    table = arch[arch.index("| # | Step | Command | Writes |"):]
    table = table[:table.index("\n\n", 10)]
    assert table.index("workspace") < table.index("EXTERNAL") < table.index("INTERNAL")
    assert "snowmig_01_structure" in table
    assert "deploy --execute" not in table, \
        "the control-plane deploy is not the INTERNAL structure step (202 can create nothing)"


# --------------------------------------------------------------------------
# "Which data-plane stages have run live" has one home and one wording.
# Five documents once gave four answers; the operator budgets the shake-out
# from this, so it must not drift.
# --------------------------------------------------------------------------

def _flat(text: str) -> str:
    return " ".join(text.split())


def test_catalog_command_does_not_call_the_standard_catalog_refused():
    text = (ROOT / "commands/snowflake-catalog.md").read_text(encoding="utf-8")
    assert "refused" not in text.lower(), \
        "the CLI creates the INTERNAL container (S4); the command said it refuses"
    assert "--catalog-type standard" in text
    assert "snowmig_01_structure" in text
    assert "container_only" in text


def test_soft_clone_command_routes_standard_structure_to_s10():
    text = (ROOT / "commands/snowflake-soft-clone.md").read_text(encoding="utf-8")
    assert "--catalog-type standard" in text
    assert "snowmig_01_structure" in text
    assert "/Workspace/Shared/" not in text and "--upload" not in text, \
        "structure does not go through the notebook upload path"


def test_the_docs_scope_the_gitignore_claim_to_the_plugin_folder():
    """The only ignore rule for snowmig-config.* lives in the plugin's own
    .gitignore, while the documented home of the file is the operator's
    working directory. "It is gitignored" was therefore a promise the
    documented location does not keep; `git add .` from another repo stages
    the password."""
    for rel in ("README.md", "skills/snowflake-migrator-bootstrap/SKILL.md"):
        flat = " ".join((ROOT / rel).read_text(encoding="utf-8").lower().split())
        assert "gitignored only inside" in flat, rel
        assert "your own `.gitignore`" in flat or "that repo's `.gitignore`" in flat, rel
        assert "it is **gitignored**, and" not in flat, f"{rel}: unscoped claim"
        assert "gitignored, `0600`" not in flat, f"{rel}: unscoped claim"


def test_the_docs_say_how_the_aidp_clis_authenticate():
    """"Auth is not configured in this plugin at all" was not true: the
    engine appends `--auth api_key --region <from the OCID>` to every `aidp`
    invocation (target/executor.py), and `oci raw-request` runs with the
    user's ~/.oci/config profile. An operator whose default profile is a
    session token, or who keeps the right key under another profile, needs
    to know which profile and which auth mode the plugin actually uses."""
    for rel in ("README.md", "skills/snowflake-migrator-bootstrap/SKILL.md"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        flat = " ".join(text.lower().split())
        assert "not configured in this plugin at all" not in flat, rel
        assert "~/.oci/config" in text, rel
        assert "`default`" in flat or "default profile" in flat, \
            f"{rel}: must name the profile `oci` runs with"
        assert "api_key" in text, f"{rel}: must say the aidp CLI is invoked with api_key auth"
        # snowmig._oci_runner inserts `--profile <aidp.oci_profile>` into
        # every `oci` argv; the docs once said the key was "not yet passed
        # through", written against the code before that runner existed.
        assert "not yet passed through" not in flat, \
            f"{rel}: aidp.oci_profile IS applied (snowmig._oci_runner)"
        assert "--profile" in text, \
            f"{rel}: must say aidp.oci_profile reaches oci as --profile"


# --------------------------------------------------------------------------
# Docs that drifted from the merged engine in the integration pass. Each pin
# names the code fact it guards, so the next change to that code fails here
# instead of silently dating the document.
# --------------------------------------------------------------------------

def test_the_write_carve_outs_name_the_execute_gate():
    """cmd_smoke: `write_probe = bool(args.write_probe and args.execute)`;
    cmd_notebook: `--upload` is a dry run without --execute and refused with
    it. ARCHITECTURE.md and the stage board listed both as bare
    opt-in writers, which is what the code did before the cli fix."""
    for rel in ("ARCHITECTURE.md", "MIGRATION-ARCHITECTURE.md",
                "skills/snowflake-stage-board/SKILL.md"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        for stale in ("only with `--write-probe`**",
                      "only with `--upload`",
                      "opt-in, `smoke --write-probe` and `notebook --upload`",
                      "(`smoke --write-probe`, `notebook --upload`)",
                      "| opt-in probe |",
                      "script → Shared/"):
            assert stale not in text, f"{rel}: {stale!r}"
        assert "--write-probe --execute" in text, \
            f"{rel}: the probe writes only with --execute"


def test_smoke_docs_state_the_three_valued_exit_contract():
    """cmd_smoke returns 1 for verdict PARTIAL -- no executed check failed,
    the four AIDP coordinates were simply not all supplied, which is the
    default first run with the example config. "Exit 1 = at least one
    failed" sent the agent hunting a connectivity fault instead of asking
    for the coordinates."""
    skill = (ROOT / "skills/snowflake-smoke-test/SKILL.md").read_text(encoding="utf-8")
    command = (ROOT / "commands/snowflake-smoke.md").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for name, text in (("smoke skill", skill), ("smoke command", command)):
        assert "partial" in text.lower(), f"{name}: name the PARTIAL verdict"
    low = " ".join(skill.lower().split())
    assert "not a pass" in low
    assert "exit 1 = at least one failed" not in low
    assert "verdict: PARTIAL" in readme, \
        "README step 5: without the coordinates the run exits 1 as PARTIAL"


def test_view_docs_carry_the_rule_counts_from_translate_rules():
    """README said "15 Snowflake-only constructs -- QUALIFY, LATERAL FLATTEN,
    IFF, ::, LISTAGG, DATEADD ... block the view" after four of those had
    become implemented rewrites and RULES had grown to 20. Every count in
    prose is bound to coverage() here, and the blocker lists may not name a
    construct the translator rewrites."""
    import sys
    sys.path.insert(0, str(ROOT / "engine"))
    from snowflake_source.dialect.translate import RULES, coverage

    c = coverage()
    # Implemented rules with no refusal form and no caveat: naming one of
    # these as a blocker is simply wrong. (`::`, DATEADD and LISTAGG have
    # refused forms, so a blocker list may legitimately mention those.)
    always_rewritten = [r.construct for r in RULES
                        if r.status == "implemented" and not r.caveat
                        and r.construct.isidentifier()
                        and r.construct not in ("LISTAGG",)]
    assert "IFF" in always_rewritten
    declared_examples = ("QUALIFY", "LATERAL FLATTEN", "DATEDIFF")
    for d in declared_examples:
        assert any(d in r.construct for r in RULES if r.status == "declared"), d

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    para = readme[readme.index("## Why a view might not migrate"):]
    para = para[:para.index("\n## ", 10)]
    flat = " ".join(para.split())
    assert "need no rewriting" not in flat, \
        "R41 rewrites view references under --bronze-catalog-prefix / schema style"
    assert not re.search(r"\d+ Snowflake-only constructs", flat)
    assert f"carries {c['total']} rules" in flat
    assert f"{c['implemented']} have a provably exact rewrite" in flat
    assert f"{c['declared']} others" in flat
    blockers = flat[flat.index(f"{c['declared']} others"):flat.index("**block**")]

    def names(construct: str, text: str) -> bool:
        # Whole token: `IFF` must not match inside `DATEDIFF`.
        return re.search(rf"\b{re.escape(construct)}\b", text) is not None

    for construct in always_rewritten:
        assert not names(construct, blockers), f"README lists {construct} as blocking"
    for d in declared_examples:
        assert names(d, blockers), d
    assert "references/dialect-translation.md" in para

    arch = (ROOT / "MIGRATION-ARCHITECTURE.md").read_text(encoding="utf-8")
    assert f"{c['implemented']} dialect rewrites" in arch
    assert f"{c['implemented']} SQL rewrites" in arch
    assert f"{c['declared']} constructs" in arch
    for rel, doc in (("README.md", readme), ("MIGRATION-ARCHITECTURE.md", arch)):
        for stale in ("6 exact", "6 implemented", "15 Snowflake-only"):
            assert stale not in doc, f"{rel}: {stale!r}"

    # The plan skill's `snowflake_only_sql` row: what blocks comes first,
    # then what is translated. IFF and :: were listed as blockers.
    skill = (ROOT / "skills/snowflake-migration-plan/SKILL.md").read_text(encoding="utf-8")
    row = next(l for l in skill.splitlines()
               if l.startswith("| `snowflake_only_sql` |"))
    head, sep, tail = row.partition("The reason names the construct")
    assert sep, "the row must say the reason names the construct"
    for construct in always_rewritten:
        assert not names(construct, head), f"plan skill lists {construct} as blocking"
    for d in declared_examples:
        assert names(d, head), d
    assert "translated, not blocked" in tail


def test_no_doc_says_the_standard_catalog_is_never_created_or_routes_to_the_notebook():
    """`catalog --catalog-type standard --execute` creates the INTERNAL
    container (S4, live-verified) and the structure is `run --job
    snowmig_01_structure` (S10); `notebook --upload --execute` is refused.
    No doc may keep the older story."""
    paths = [ROOT / p for p in ("README.md", "ARCHITECTURE.md")]
    paths += sorted((ROOT / "commands").glob("*.md"))
    paths += sorted((ROOT / "skills").glob("*/SKILL.md"))
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for stale in ("never created by this plugin",
                      "notebook`, run on the cluster",
                      "run it on the cluster",
                      # catalog_result.json's note routes to S10 now
                      "pointer to the notebook path",
                      # three stages write, not one
                      "single writing stage"):
            assert stale not in text, f"{path.relative_to(ROOT)}: {stale!r}"


def test_notebook_command_and_skill_do_not_promise_upload_or_execution():
    """cmd_notebook never places anything on the workspace: `--upload` is a
    dry run and `--upload --execute` exits 1 with "Upload refused". The command promised "place it in the AIDP workspace, ready for you
    to execute" and the skill "generate, upload, run"."""
    text = (ROOT / "commands/snowflake-notebook.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "snowmig_01_structure" in text
    for stale in ("then upload", "ready for you to execute", "ask before executing"):
        assert stale not in low, stale
    for line in text.splitlines():
        if "--upload" in line:
            assert "refused" in line or "dry run" in line, line
    assert "/Workspace/Shared/" not in text
    skill = (ROOT / "skills/snowflake-clone-notebook/SKILL.md").read_text(encoding="utf-8")
    flat = " ".join(skill.lower().split())
    for stale in ("three steps: generate, upload, run",
                  "place it in the workspace shared directory",
                  "lands at `/workspace/shared/"):
        assert stale not in flat, stale
    assert "snowmig_01_structure" in skill
    assert "refused" in flat and "dry run" in flat


def test_bootstrap_skill_describes_the_launcher_as_shipped():
    """bin/snowmig parses no flags of its own and persists nothing: it runs
    the first interpreter that imports the deps, else a throwaway venv under
    $TMPDIR that its EXIT trap removes. The skill told the agent to run
    `--bootstrap`, described `--python`, and placed the venv under
    XDG_DATA_HOME -- a design that never shipped."""
    text = (ROOT / "skills/snowflake-migrator-bootstrap/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    for stale in ("--bootstrap", "SNOWMIG_VENV", "XDG_DATA_HOME", "`--python`"):
        assert stale not in text, stale
    assert "throwaway" in low
    assert "removed on exit" in low
    launcher = (ROOT / "bin/snowmig").read_text(encoding="utf-8")
    for token in ("--bootstrap", "SNOWMIG_VENV"):
        assert token not in launcher, f"the launcher grew {token}; update the skill"


def test_the_runbook_names_refresh_notebooks_for_a_params_rewrite():
    """provisioning: `keep_existing = reuse_existing and not
    refresh_notebooks` -- with --reuse-existing alone a stage notebook already
    on the workspace is KEPT (its PARAMS cell may have been edited in the
    console). S10 said --reuse-existing rewrites and re-uploads it."""
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    flat = " ".join(text.split())
    assert "--reuse-existing --refresh-notebooks` rewrites it" in flat
    assert "`provision --execute --reuse-existing` rewrites it" not in flat


_ENV_COORDS_TOKENS = re.compile(
    r"AIDP_REGION|AIDP_DATALAKE_OCID|AIDP_WORKSPACE_ID|AIDP_CLUSTER_ID|"
    r"AIDP_BASE\b|ANTHROPIC_API_KEY|OCI_TENANCY_OCID|--lake-ocid|--workspace-id\b")


def test_no_prose_offers_an_environment_variable_destination():
    """The destination is the aidp: block of snowmig-config.yaml or the four
    flags; nothing is read from the environment (ARCHITECTURE.md I5,
    target/coords.py). references/env-coords.template.md told the operator
    to export AIDP_* variables and an ANTHROPIC_API_KEY no module reads, and
    named --lake-ocid/--workspace-id/--cluster flags no subcommand has.
    Prose only: target/executor.py legitimately passes --workspace-id to the
    aidp CLI."""
    assert not (ROOT / "references/env-coords.template.md").exists()
    paths = sorted((ROOT / "references").glob("*.md"))
    paths += sorted((ROOT / "skills").glob("*/SKILL.md"))
    paths += sorted((ROOT / "commands").glob("*.md"))
    paths += [ROOT / "README.md", ROOT / "ARCHITECTURE.md"]
    offenders = []
    for path in paths:
        hit = _ENV_COORDS_TOKENS.search(path.read_text(encoding="utf-8"))
        if hit:
            offenders.append(f"{path.relative_to(ROOT)}: {hit.group(0)}")
    assert offenders == [], offenders


def test_the_docs_describe_the_derived_source_config_copy():
    """provisioning.source_config_payload uploads the `snowflake:` block only,
    re-serialised as JSON, to plan/<config stem>.json; the operator's YAML
    never travels and the aidp: block stays on the laptop. README, the
    provision skill and the scripts README described the whole-file upload
    (and a default path, plan/snowmig-config.yaml, that is never created)."""
    readme = " ".join((ROOT / "README.md").read_text(encoding="utf-8").split())
    assert "hand them JSON if the cluster image has no PyYAML" not in readme
    assert "plan/<config stem>.json" in readme
    provision = (ROOT / "skills/snowflake-provision-environment/SKILL.md").read_text(encoding="utf-8")
    flat = " ".join(provision.split())
    assert "`snowflake:` block" in flat and "`aidp:` block is not copied" in flat
    scripts = (ROOT / "data-migration-scripts/README.md").read_text(encoding="utf-8")
    assert "plan/snowmig-config.yaml" not in scripts, "the derived copy is JSON"
    assert "plan/snowmig-config.json" in scripts
