#!/usr/bin/env python3
"""
deploy.py — Build, push, and deploy the AIDP Agent Chat UI to OCI Container Instances.

Fully interactive: auto-discovers everything from ~/.oci/config, prompts for
confirmation, creates missing resources (auth tokens, VCN, container instance).

Prerequisites:
  - Docker + Docker Buildx
  - An OCI config at ~/.oci/config
  - A deployed AIDP agent endpoint URL
  - A Dynamic Group + Policy granting the deployed Container Instance permission
    to invoke your agent endpoint (because AUTH = resource_principal in-container).
    Example matching rule:
        ALL {resource.type = 'computecontainerinstance', resource.compartment.id = '<COMPARTMENT_OCID>'}
    Example policy:
        Allow dynamic-group <DG_NAME> to use genai-agent-endpoint in compartment <COMPARTMENT_NAME>
  - The Dynamic Group also added as a MEMBER of the AIDP Workbench that hosts
    the agent (Workbench → Roles → Admin). IAM alone is not enough — the AIDP
    gateway enforces its own Workbench auth and returns HTTP 404 on chat if
    the resource principal isn't a workbench member.

Usage:
    python3 deploy.py                 # interactive; checks IAM, refuses if missing
    python3 deploy.py --yes           # skip confirmations (uses defaults + env vars)
    python3 deploy.py --create-iam    # also try to create the DG + Policy if missing
                                      # (needs tenancy-level perms; falls back to manual)

Run from the web_ui/ directory.
"""

import base64
import os
import subprocess
import sys
import time

import oci
from oci.regions import REGIONS_SHORT_NAMES

_REGION_TO_SHORT = {v: k for k, v in REGIONS_SHORT_NAMES.items()}
_AUTO = "--yes" in sys.argv
_CREATE_IAM = "--create-iam" in sys.argv

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))           # web_ui/
_BUILD_CONTEXT = os.path.dirname(_SCRIPT_DIR)                      # aidp_chat_client/
_DOCKERFILE = os.path.join(_SCRIPT_DIR, "Dockerfile")
_DEPLOY_CONFIG = os.path.join(_SCRIPT_DIR, "config-deploy.yaml")


# ── Config loading ───────────────────────────────────────────────────

def _load_deploy_config():
    """Load config-deploy.yaml (sibling to deploy.py) if present, and bridge
    its values to the env-var keys the rest of the script already reads from.
    Existing env vars take precedence — letting users override the file at
    invocation time without editing it. Falls through gracefully if the
    file or PyYAML is missing."""
    if not os.path.exists(_DEPLOY_CONFIG):
        return
    try:
        import yaml
    except ImportError:
        print(f"  Note: config-deploy.yaml found but PyYAML isn't installed; ignoring it.")
        print(f"        pip install pyyaml  # to enable")
        return

    with open(_DEPLOY_CONFIG) as f:
        cfg = yaml.safe_load(f) or {}

    # YAML key path → env-var name. Only set env if the YAML provides a
    # non-empty value AND the env var isn't already set by the caller.
    mapping = {
        ("agent", "url"):                "AIDP_AGENT_URL",
        ("deploy", "region"):            "OCI_REGION",
        ("deploy", "compartment_id"):    "DEPLOY_COMPARTMENT_ID",
        ("deploy", "display_name"):      "DEPLOY_DISPLAY_NAME",
        ("deploy", "shape"):             "DEPLOY_SHAPE",
        ("deploy", "cpus"):              "DEPLOY_CPUS",
        ("deploy", "memory_gb"):         "DEPLOY_MEMORY_GB",
        ("container", "repo"):           "OCIR_REPO",
        ("container", "tag"):            "OCIR_TAG",
        ("iam", "policy_resource"):      "AIDP_POLICY_RESOURCE",
        ("ocir", "auth_token"):          "OCIR_AUTH_TOKEN",
    }
    applied = []
    for path, env_key in mapping.items():
        node = cfg
        for p in path:
            if not isinstance(node, dict):
                node = None
                break
            node = node.get(p)
        if node not in (None, "") and not os.environ.get(env_key):
            os.environ[env_key] = str(node)
            applied.append(env_key)

    if applied:
        print(f"  Loaded {len(applied)} value(s) from config-deploy.yaml: {', '.join(applied)}")


_load_deploy_config()


# ── Helpers ──────────────────────────────────────────────────────────

def prompt(label: str, default: str = "", secret: bool = False) -> str:
    """Prompt user for input with a default value. Returns default if --yes."""
    if _AUTO and default:
        print(f"  {label}: {default}")
        return default
    suffix = f" [{default}]" if default else ""
    if secret:
        import getpass
        val = getpass.getpass(f"  {label}{suffix}: ").strip()
    else:
        val = input(f"  {label}{suffix}: ").strip()
    return val or default


def run_cmd(cmd: list, input_data: str | None = None) -> None:
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, input=input_data, text=True)
    if result.returncode != 0:
        print(f"  Command failed (exit code {result.returncode})")
        sys.exit(result.returncode)


def wait_for_state(get_fn, target_states, max_wait=300, poll_interval=10):
    start = time.time()
    while time.time() - start < max_wait:
        resource = get_fn()
        state = resource.data.lifecycle_state
        print(f"    State: {state}")
        if state in target_states:
            return resource.data
        if state in ("FAILED", "TERMINATED", "DELETED"):
            raise RuntimeError(f"Resource entered {state} state")
        time.sleep(poll_interval)
    raise RuntimeError(f"Timed out waiting for state {target_states}")


# ── Discovery ────────────────────────────────────────────────────────

def discover_config():
    """Auto-discover all config from ~/.oci/config + OCI APIs, prompt for the rest."""
    print("\n── Configuration ──────────────────────────────────────────")
    print("  Loading ~/.oci/config...")

    oci_config = oci.config.from_file()
    oci.config.validate_config(oci_config)

    tenancy_id = os.environ.get("OCI_TENANCY_ID") or oci_config["tenancy"]
    user_ocid = oci_config["user"]

    print(f"  Tenancy: {tenancy_id}")
    print(f"  User:    {user_ocid}")
    # If region came from config-deploy.yaml or env, take it as-is.
    # Only prompt when neither was set, falling back to ~/.oci/config region.
    region = os.environ.get("OCI_REGION")
    if not region:
        region = prompt("Region (e.g. us-ashburn-1, sa-saopaulo-1, eu-frankfurt-1)", oci_config["region"])
    print(f"  Region:  {region}")
    # Propagate the choice so every regional OCI client (VCN, container,
    # object storage) created below targets the selected region rather than
    # whatever ~/.oci/config defaults to.
    oci_config["region"] = region

    # OCI Identity create/update/delete (auth tokens, dynamic groups, policies)
    # MUST go through the tenancy's home region — other regions only host read
    # replicas of identity resources. Discover the home region and pin the
    # IdentityClient to it so IAM ops succeed even when deploying elsewhere.
    bootstrap = oci.identity.IdentityClient(oci_config)
    tenancy = bootstrap.get_tenancy(tenancy_id).data
    # home_region_key is a short code like "IAD" / "GRU"; REGIONS_SHORT_NAMES
    # maps short → full name (the inverse of _REGION_TO_SHORT, which we use
    # elsewhere for region name → short code lookups).
    home_region_key = tenancy.home_region_key.lower()
    home_region = REGIONS_SHORT_NAMES.get(home_region_key)
    if not home_region:
        print(f"  ERROR: SDK does not recognize home region key {home_region_key!r}.")
        print("         Update the oci package or set OCI_REGION to the home region manually.")
        sys.exit(1)
    tenancy_name = tenancy.name

    oci_config_home = dict(oci_config)
    oci_config_home["region"] = home_region
    identity = oci.identity.IdentityClient(oci_config_home)

    print(f"  Tenancy name: {tenancy_name}")
    print(f"  Home region:  {home_region}  (used for auth tokens & IAM ops)")

    region_key = _REGION_TO_SHORT.get(region)
    if not region_key:
        region_key = prompt("OCIR region key (e.g. iad, gru, fra)")
    print(f"  Region key: {region_key}")

    namespace = os.environ.get("OCIR_NAMESPACE")
    if not namespace:
        os_client = oci.object_storage.ObjectStorageClient(oci_config)
        namespace = os_client.get_namespace().data
    print(f"  OCIR namespace: {namespace}")

    # OCIR username format depends on user type:
    #  - IDCS-federated users (most modern tenancies): user.name already contains
    #    "oracleidentitycloudservice/<email>" — prepend only the namespace.
    #  - Local IAM users: user.name is bare — prepend namespace + IDCS prefix.
    # Check for the embedded "/" to disambiguate.
    username = os.environ.get("OCIR_USERNAME")
    if not username:
        user_data = identity.get_user(user_ocid).data
        if "/" in user_data.name:
            username = f"{namespace}/{user_data.name}"
        else:
            username = f"{namespace}/oracleidentitycloudservice/{user_data.name}"
    print(f"  OCIR username: {username}")

    auth_token = os.environ.get("OCIR_AUTH_TOKEN")
    token_was_just_created = False
    if not auth_token:
        print("\n  OCIR Auth Token needed for Docker registry login.")
        print("  Checking YOUR existing auth tokens (the API only lists this user's tokens)...")
        tokens = identity.list_auth_tokens(user_ocid).data
        active_tokens = [t for t in tokens if t.lifecycle_state == "ACTIVE"]
        print(f"  Found {len(active_tokens)} of your active token(s) — OCI limit is 2 per user.")
        for i, t in enumerate(active_tokens, 1):
            desc = t.description or "(no description)"
            created = t.time_created.strftime("%Y-%m-%d %H:%M") if t.time_created else "?"
            print(f"    {i}) {desc} — created {created}")
        if len(active_tokens) >= 2:
            print("  Manage tokens at: Identity → Users → <your user> → Auth Tokens")

        # In --yes mode with 2/2 tokens we refuse to silently delete an existing
        # token — it might be in use by another tool (CI, backups, etc.) and
        # rotating it without consent is destructive.
        if len(active_tokens) >= 2 and _AUTO:
            print()
            print("  ERROR: 2/2 active OCIR auth tokens (OCI per-user limit reached).")
            print("         Cannot safely create a new one in --yes mode without")
            print("         risking another tool that depends on an existing token.")
            print("         Resolve by either:")
            print("           (a) Set OCIR_AUTH_TOKEN env var to a known token value, or")
            print("           (b) Delete an unused token (OCI Console:")
            print("               Identity → Users → your user → Auth Tokens),")
            print("               then re-run.")
            sys.exit(1)

        choice = prompt("Create a new auth token? (y/n)", "y" if len(active_tokens) < 2 else "n")
        if choice.lower() in ("y", "yes"):
            if len(active_tokens) >= 2:
                print("  You already have 2 active tokens (OCI limit).")
                print("  Delete one first, or provide an existing token.")
                auth_token = prompt("Paste existing OCIR auth token", secret=True)
            else:
                print("  Creating auth token...")
                resp = identity.create_auth_token(
                    oci.identity.models.CreateAuthTokenDetails(
                        description="aidp-agent-chat-ui deploy"
                    ),
                    user_ocid,
                )
                auth_token = resp.data.token
                token_was_just_created = True
                print(f"  Auth token created. Save this — it won't be shown again!")
                print(f"  Token: {auth_token}")
        else:
            auth_token = prompt("Paste existing OCIR auth token", secret=True)

    if not auth_token:
        print("ERROR: Auth token is required.")
        sys.exit(1)

    print("\n  Selecting compartment to deploy into...")
    compartment_id = os.environ.get("DEPLOY_COMPARTMENT_ID")
    if not compartment_id:
        comps = identity.list_compartments(
            tenancy_id, lifecycle_state="ACTIVE", access_level="ANY",
            compartment_id_in_subtree=True, limit=500,
        ).data
        visible = 30
        print(f"  Available compartments ({len(comps)} total):")
        print(f"    0) {tenancy_name} (root)")
        for i, c in enumerate(comps[:visible], 1):
            print(f"    {i}) {c.name}")
        if len(comps) > visible:
            print(f"    ... and {len(comps) - visible} more — for those, paste the OCID directly below")

        choice = prompt(
            "Select compartment number, or paste a compartment/tenancy OCID",
            "0",
        ).strip()

        if choice.startswith("ocid1.compartment.") or choice.startswith("ocid1.tenancy."):
            compartment_id = choice
        else:
            try:
                idx = int(choice)
            except ValueError:
                print(f"  Invalid input: {choice!r} is neither a number nor an OCID.")
                sys.exit(1)
            if idx == 0:
                compartment_id = tenancy_id
            elif 1 <= idx <= len(comps):
                compartment_id = comps[idx - 1].id
            else:
                print(f"  Invalid selection: {idx} (must be 0–{len(comps)})")
                sys.exit(1)
    print(f"  Compartment: {compartment_id}")

    # AIDP agent endpoint URL — copy from the AIDP console after deploying an agent.
    agent_url = os.environ.get("AIDP_AGENT_URL")
    if not agent_url:
        agent_url = prompt(
            "AIDP agent /chat URL (paste from AIDP console)",
            "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat",
        )
    print(f"  Agent URL: {agent_url}")

    repo = os.environ.get("OCIR_REPO", "aidp-agent-chat-ui")
    tag = os.environ.get("OCIR_TAG", "latest")
    shape = os.environ.get("DEPLOY_SHAPE", "CI.Standard.E4.Flex")
    cpus = os.environ.get("DEPLOY_CPUS", "1")
    memory_gb = os.environ.get("DEPLOY_MEMORY_GB", "2")
    display_name = os.environ.get("DEPLOY_DISPLAY_NAME", "aidp-agent-chat-ui")

    ad = os.environ.get("DEPLOY_AD")
    if not ad:
        # Availability domains are regional — must query in the deploy region,
        # not the home region where `identity` is pinned.
        deploy_identity = oci.identity.IdentityClient(oci_config)
        ad = deploy_identity.list_availability_domains(compartment_id).data[0].name

    registry = f"{region_key}.ocir.io"
    full_image = f"{registry}/{namespace}/{repo}:{tag}"

    return {
        "oci_config": oci_config,            # deploy region — for VCN, container, object storage
        "oci_config_home": oci_config_home,  # home region — for Identity CRUD (auth tokens, DGs, policies)
        "tenancy_id": tenancy_id,
        "region": region,
        "region_key": region_key,
        "namespace": namespace,
        "username": username,
        "auth_token": auth_token,
        "token_was_just_created": token_was_just_created,
        "compartment_id": compartment_id,
        "registry": registry,
        "full_image": full_image,
        "shape": shape,
        "cpus": cpus,
        "memory_gb": memory_gb,
        "display_name": display_name,
        "ad": ad,
        "agent_url": agent_url,
    }


# ── IAM (Dynamic Group + Policy) ─────────────────────────────────────

def check_or_create_iam(cfg, create=False):
    """
    Verify the Dynamic Group + Policy exist that let the deployed Container
    Instance call the agent endpoint via resource principal.

    With create=False (default) this is read-only — only `inspect` on identity
    is needed. With create=True it tries to create whatever is missing,
    falling back to printing manual instructions if perms or Identity Domains
    get in the way.

    Returns True if IAM is fully in place, False otherwise.
    """
    print("\n── Step 0: IAM check (Dynamic Group + Policy) ─────────────")

    # Pin to home region — Identity create/update/delete on DGs and policies
    # only works there.
    identity = oci.identity.IdentityClient(cfg["oci_config_home"])
    cid = cfg["compartment_id"]
    name = cfg["display_name"]
    dg_name = f"{name}-dg"
    policy_name = f"{name}-policy"
    matching_rule = (
        f"ALL {{resource.type = 'computecontainerinstance', "
        f"resource.compartment.id = '{cid}'}}"
    )
    # AIDP agent endpoints sit behind the AIDP Workbench gateway, which gates
    # access via Workbench-level auth. The IAM family that controls Workbench
    # access is 'ai-data-platforms'. Granting this is the IAM half of the
    # equation — the principal STILL needs to be added as a member of the
    # specific AIDP Workbench inside the AIDP UI (Workbench → Roles).
    # Override via env var if Oracle changes the family name.
    policy_resource = os.environ.get(
        "AIDP_POLICY_RESOURCE", "ai-data-platforms"
    )

    # Will be filled in once the DG is confirmed/created. The OCID-based form
    # works in both classic and Identity Domains tenancies; the bare-name form
    # often fails to resolve in Identity Domains and produces the misleading
    # "No permissions found" parser error.
    dg_ocid = None

    def build_statement(subject):
        return (
            f"Allow dynamic-group {subject} to use {policy_resource} "
            f"in compartment id {cid}"
        )

    def print_manual():
        # Manual instructions use the readable name form — the console resolves
        # it correctly because it shows you a picker.
        readable_statement = build_statement(dg_name)
        print()
        print("  Manual setup needed in the OCI Console BEFORE the container can call the agent:")
        print()
        print("  1. Identity → Dynamic Groups → Create:")
        print(f"       Name:           {dg_name}")
        print(f"       Matching rule:  {matching_rule}")
        print()
        print("  2. Identity → Policies → Create in your compartment:")
        print(f"       Name:        {policy_name}")
        print(f"       Statement:   {readable_statement}")
        print()
        print("  Note: tenancies using Identity Domains manage Dynamic Groups inside")
        print("  the domain — use the Identity Domains console instead of classic IAM.")
        print()
        print(f"  Tip: if OCI rejects the resource type {policy_resource!r}, paste the")
        print("       statement into the Console policy editor — it autocompletes valid")
        print("       resource types. Re-run with: AIDP_POLICY_RESOURCE=<correct> python3 deploy.py")
        print()
        print("  After setup, re-run: python3 deploy.py")
        print("  Or to auto-create now (needs tenancy admin): python3 deploy.py --create-iam")

    # ── Dynamic Group ──
    dg_ok = False
    try:
        dgs = identity.list_dynamic_groups(cfg["tenancy_id"]).data
        existing_dg = next(
            (d for d in dgs if d.name == dg_name and d.lifecycle_state == "ACTIVE"),
            None,
        )
        if existing_dg:
            print(f"  Dynamic Group OK: {existing_dg.name}")
            dg_ok = True
            dg_ocid = existing_dg.id
        else:
            print(f"  Dynamic Group missing: {dg_name}")
    except oci.exceptions.ServiceError as e:
        print(f"  Cannot list Dynamic Groups ({e.code}). Skipping check.")
        print_manual()
        return False

    if not dg_ok and create:
        try:
            print(f"  → Creating Dynamic Group: {dg_name}")
            resp = identity.create_dynamic_group(
                oci.identity.models.CreateDynamicGroupDetails(
                    compartment_id=cfg["tenancy_id"],
                    name=dg_name,
                    matching_rule=matching_rule,
                    description=f"For {name} Container Instance to call AIDP agents",
                )
            )
            dg_ok = True
            dg_ocid = resp.data.id
            print("    Created.")
        except oci.exceptions.ServiceError as e:
            print(f"    Could not create Dynamic Group: {e.code} — {e.message}")
            print_manual()
            return False

    if not dg_ok:
        print_manual()
        return False

    # Use the OCID form for programmatic statement creation — robust against
    # Identity Domains and against newly-created DGs whose name hasn't propagated.
    policy_statement = build_statement(f"id {dg_ocid}")

    # ── Policy ──
    policy_ok = False
    try:
        policies = identity.list_policies(cid).data
        target = policy_statement.lower()
        for p in policies:
            if any(target in s.lower() for s in p.statements):
                print(f"  Policy OK: {p.name} (already grants the required statement)")
                policy_ok = True
                break
        if not policy_ok:
            print(f"  Policy missing: no policy in this compartment grants the required access")
    except oci.exceptions.ServiceError as e:
        print(f"  Cannot list Policies ({e.code}). Skipping check.")
        print_manual()
        return False

    if not policy_ok and create:
        try:
            existing_named = next(
                (p for p in policies if p.name == policy_name), None
            )
            if existing_named:
                print(f"  → Policy '{policy_name}' exists; appending statement")
                identity.update_policy(
                    existing_named.id,
                    oci.identity.models.UpdatePolicyDetails(
                        statements=list(existing_named.statements) + [policy_statement],
                    ),
                )
            else:
                print(f"  → Creating Policy: {policy_name}")
                identity.create_policy(
                    oci.identity.models.CreatePolicyDetails(
                        compartment_id=cid,
                        name=policy_name,
                        description=f"Grants {dg_name} access to AIDP agent endpoints",
                        statements=[policy_statement],
                    )
                )
            policy_ok = True
            print("    Done.")
        except oci.exceptions.ServiceError as e:
            print(f"    Could not create/update Policy: {e.code} — {e.message}")
            if "no permissions found" in (e.message or "").lower():
                print()
                print("    OCI's parser couldn't resolve any permissions in this statement —")
                print("    the most common cause is the resource family name being wrong.")
                print(f"    Statement attempted:")
                print(f"      {policy_statement}")
                print()
                print("    Paste it into the OCI Console policy editor (Identity → Policies →")
                print("    Create) — it will autocomplete valid resource family names. Then")
                print(f"    re-run with: AIDP_POLICY_RESOURCE=<the right one> python3 deploy.py --create-iam")
            print_manual()
            return False

    if not policy_ok:
        print_manual()
        return False

    print("  IAM is in place.")
    # Stash the DG OCID for the final post-deploy reminder.
    cfg["dg_ocid"] = dg_ocid
    return True


# ── Docker build & push ──────────────────────────────────────────────

def _docker_login_with_retry(cfg, max_wait_seconds: int = 90, interval: int = 5) -> None:
    """Try `docker login`, retrying briefly on Unauthorized.

    Newly-created OCIR auth tokens take ~30-60s to propagate, so an immediate
    login using a fresh token returns 401. Retrying with a short backoff is
    safe and sidesteps the race. We `docker logout` before each attempt
    because Rancher Desktop's credential cache can hold a stale entry that
    makes valid tokens appear unauthorized.

    Login uses `-p` instead of `--password-stdin` because Rancher Desktop's
    Docker CLI mishandles stdin from subprocess. The token never leaves the
    process — the CLI warning about ps/history visibility is not a concern.
    """
    deadline = time.time() + max_wait_seconds
    attempt = 1
    while True:
        subprocess.run(["docker", "logout", cfg["registry"]],
                       capture_output=True, check=False)
        result = subprocess.run(
            ["docker", "login", cfg["registry"],
             "--username", cfg["username"], "-p", cfg["auth_token"]],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            if attempt > 1:
                print(f"  Login succeeded on attempt {attempt}.")
            else:
                print("  Login succeeded.")
            return
        if time.time() >= deadline:
            print(f"  Docker login still failing after {max_wait_seconds}s.")
            print(f"  stderr: {(result.stderr or '').strip()[:300]}")
            sys.exit(1)
        is_propagation = (cfg.get("token_was_just_created") and
                          "unauthorized" in (result.stderr or "").lower())
        reason = ("token still propagating" if is_propagation
                  else (result.stderr or "").strip().splitlines()[-1][:120])
        print(f"  Attempt {attempt} failed ({reason}). Retrying in {interval}s...")
        time.sleep(interval)
        attempt += 1


def build_and_push(cfg):
    print("\n── Step 1: Build & Push Docker Image ──────────────────────")
    print(f"  Image: {cfg['full_image']}")

    print("\n→ Logging in to OCIR...")
    _docker_login_with_retry(cfg)

    # Build context is aidp_chat_client/ so the Dockerfile can install the
    # parent library alongside the web_ui sources.
    print("\n→ Building image (linux/amd64)...")
    run_cmd([
        "docker", "buildx", "build",
        "--platform", "linux/amd64",
        "-t", cfg["full_image"],
        "-f", _DOCKERFILE,
        _BUILD_CONTEXT,
    ])

    print("\n→ Pushing image...")
    run_cmd(["docker", "push", cfg["full_image"]])
    print("  Image pushed.")


# ── Networking ───────────────────────────────────────────────────────

def _find_or_create(list_fn, create_fn, wait_get_fn, label):
    """Generic find-or-create with AVAILABLE wait."""
    existing = list_fn()
    active = [r for r in existing if r.lifecycle_state == "AVAILABLE"]
    if active:
        print(f"  {label} exists: {active[0].id}")
        return active[0]
    print(f"  Creating {label}...")
    resp = create_fn()
    result = wait_for_state(lambda: wait_get_fn(resp.data.id), ["AVAILABLE"])
    print(f"  {label} created: {result.id}")
    return result


def setup_networking(cfg):
    print("\n── Step 2: Networking ──────────────────────────────────────")
    vn = oci.core.VirtualNetworkClient(cfg["oci_config"])
    cid = cfg["compartment_id"]
    name = cfg["display_name"]

    vcn = _find_or_create(
        lambda: vn.list_vcns(cid, display_name=f"{name}-vcn").data,
        lambda: vn.create_vcn(oci.core.models.CreateVcnDetails(
            compartment_id=cid, display_name=f"{name}-vcn",
            cidr_blocks=["10.0.0.0/16"], dns_label="agentchat",
        )),
        lambda id: vn.get_vcn(id),
        "VCN",
    )

    igw = _find_or_create(
        lambda: vn.list_internet_gateways(cid, vcn_id=vcn.id, display_name=f"{name}-igw").data,
        lambda: vn.create_internet_gateway(oci.core.models.CreateInternetGatewayDetails(
            compartment_id=cid, vcn_id=vcn.id, display_name=f"{name}-igw", is_enabled=True,
        )),
        lambda id: vn.get_internet_gateway(id),
        "Internet Gateway",
    )

    rt = _find_or_create(
        lambda: vn.list_route_tables(cid, vcn_id=vcn.id, display_name=f"{name}-rt").data,
        lambda: vn.create_route_table(oci.core.models.CreateRouteTableDetails(
            compartment_id=cid, vcn_id=vcn.id, display_name=f"{name}-rt",
            route_rules=[oci.core.models.RouteRule(
                network_entity_id=igw.id, destination="0.0.0.0/0", destination_type="CIDR_BLOCK",
            )],
        )),
        lambda id: vn.get_route_table(id),
        "Route Table",
    )

    sl = _find_or_create(
        lambda: vn.list_security_lists(cid, vcn_id=vcn.id, display_name=f"{name}-sl").data,
        lambda: vn.create_security_list(oci.core.models.CreateSecurityListDetails(
            compartment_id=cid, vcn_id=vcn.id, display_name=f"{name}-sl",
            ingress_security_rules=[
                oci.core.models.IngressSecurityRule(
                    source="0.0.0.0/0", source_type="CIDR_BLOCK", protocol="6",
                    tcp_options=oci.core.models.TcpOptions(
                        destination_port_range=oci.core.models.PortRange(min=5001, max=5001)),
                ),
            ],
            egress_security_rules=[
                oci.core.models.EgressSecurityRule(
                    destination="0.0.0.0/0", destination_type="CIDR_BLOCK", protocol="all",
                ),
            ],
        )),
        lambda id: vn.get_security_list(id),
        "Security List",
    )

    subnet = _find_or_create(
        lambda: vn.list_subnets(cid, vcn_id=vcn.id, display_name=f"{name}-subnet").data,
        lambda: vn.create_subnet(oci.core.models.CreateSubnetDetails(
            compartment_id=cid, vcn_id=vcn.id, display_name=f"{name}-subnet",
            cidr_block="10.0.1.0/24", route_table_id=rt.id,
            security_list_ids=[sl.id], dns_label="chatsub",
            prohibit_public_ip_on_vnic=False,
        )),
        lambda id: vn.get_subnet(id),
        "Subnet",
    )

    return subnet.id


# ── Container Instance ───────────────────────────────────────────────

def deploy_container(cfg, subnet_id):
    print("\n── Step 3: Container Instance ──────────────────────────────")
    ci = oci.container_instances.ContainerInstanceClient(cfg["oci_config"])
    cid = cfg["compartment_id"]
    name = cfg["display_name"]

    existing = ci.list_container_instances(cid, display_name=name).data.items
    active = [i for i in existing if i.lifecycle_state in ("ACTIVE", "CREATING", "UPDATING")]
    if active:
        print(f"  Replacing existing instance: {active[0].id}")
        ci.delete_container_instance(active[0].id)
        try:
            wait_for_state(
                lambda: ci.get_container_instance(active[0].id),
                ["DELETED"], max_wait=120,
            )
        except Exception:
            pass

    print(f"  Creating container instance...")
    print(f"    Shape: {cfg['shape']} ({cfg['cpus']} OCPU, {cfg['memory_gb']} GB)")
    print(f"    Image: {cfg['full_image']}")

    resp = ci.create_container_instance(
        oci.container_instances.models.CreateContainerInstanceDetails(
            display_name=name,
            compartment_id=cid,
            availability_domain=cfg["ad"],
            shape=cfg["shape"],
            shape_config=oci.container_instances.models.CreateContainerInstanceShapeConfigDetails(
                ocpus=float(cfg["cpus"]),
                memory_in_gbs=float(cfg["memory_gb"]),
            ),
            containers=[
                oci.container_instances.models.CreateContainerDetails(
                    display_name=f"{name}-app",
                    image_url=cfg["full_image"],
                    environment_variables={
                        "AIDP_AGENT_URL": cfg["agent_url"],
                        "AIDP_AUTH": "resource_principal",
                        "OCI_REGION": cfg["region"],
                    },
                )
            ],
            vnics=[
                oci.container_instances.models.CreateContainerVnicDetails(
                    subnet_id=subnet_id,
                    is_public_ip_assigned=True,
                )
            ],
            image_pull_secrets=[
                oci.container_instances.models.CreateBasicImagePullSecretDetails(
                    registry_endpoint=cfg["registry"],
                    secret_type="BASIC",
                    # OCI requires BASIC pull-secret credentials base64-encoded.
                    username=base64.b64encode(cfg["username"].encode()).decode(),
                    password=base64.b64encode(cfg["auth_token"].encode()).decode(),
                )
            ],
            container_restart_policy="ALWAYS",
        )
    )

    print("  Waiting for ACTIVE state...")
    instance = wait_for_state(
        lambda: ci.get_container_instance(resp.data.id),
        ["ACTIVE"], max_wait=600, poll_interval=15,
    )
    return instance


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  AIDP Agent Chat UI  |  Deploy to OCI Container Instances")
    print("=" * 60)

    cfg = discover_config()

    if not check_or_create_iam(cfg, create=_CREATE_IAM):
        print("\n  Aborting — set up the Dynamic Group + Policy and re-run.")
        sys.exit(1)

    print(f"""
── Summary ────────────────────────────────────────────────
  Region:        {cfg['region']} ({cfg['region_key']})
  Compartment:   {cfg['compartment_id']}
  Image:         {cfg['full_image']}
  Shape:         {cfg['shape']} ({cfg['cpus']} OCPU, {cfg['memory_gb']} GB)
  AD:            {cfg['ad']}
  Display Name:  {cfg['display_name']}
  Agent URL:     {cfg['agent_url']}
""")

    if not _AUTO:
        confirm = input("  Proceed? [Y/n] ").strip().lower()
        if confirm in ("n", "no"):
            print("  Cancelled.")
            sys.exit(0)

    build_and_push(cfg)
    subnet_id = setup_networking(cfg)
    instance = deploy_container(cfg, subnet_id)

    print("\n── Done ───────────────────────────────────────────────────")

    def _print_workbench_reminder():
        dg = cfg.get("dg_ocid", "<your dynamic group OCID>")
        print("""
  ┌─ ONE MORE STEP ─ AIDP Workbench membership ─────────────────────────┐
  │                                                                     │
  │  IAM alone is not enough. The AIDP gateway also enforces            │
  │  Workbench-level auth — the deployed Container Instance's resource  │
  │  principal must be a MEMBER of the AIDP Workbench that hosts your   │
  │  agent. Without this, the chat UI will get HTTP 404 from the        │
  │  gateway even though IAM is correctly set up.                       │
  │                                                                     │
  │  How to add it:                                                     │
  │    1. Open the AIDP Workbench that hosts your agent                 │
  │    2. Go to the Roles tab in the left menu                          │
  │    3. Add the dynamic-group OCID below as Admin (simplest role)     │
  │                                                                     │""")
        print(f"  │  DG OCID:")
        print(f"  │    {dg}")
        print("""  │                                                                     │
  └─────────────────────────────────────────────────────────────────────┘
""")

    try:
        vn = oci.core.VirtualNetworkClient(cfg["oci_config"])
        ci = oci.container_instances.ContainerInstanceClient(cfg["oci_config"])
        full = ci.get_container_instance(instance.id).data
        for vnic_info in (full.vnics or []):
            vnic = vn.get_vnic(vnic_info.vnic_id).data
            if vnic.public_ip:
                print(f"""
  Container instance is running!

  Public IP:  {vnic.public_ip}
  Access at:  http://{vnic.public_ip}:5001

  Note: this is HTTP, not HTTPS, and the proxy has no auth in front of it.
  For production, place a load balancer with HTTPS + user auth in front.
""")
                _print_workbench_reminder()
                return
    except Exception:
        pass
    print(f"\n  Container instance created: {instance.id}")
    print("  Check OCI Console for the public IP.")
    _print_workbench_reminder()


if __name__ == "__main__":
    main()
