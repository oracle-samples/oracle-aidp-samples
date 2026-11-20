#!/usr/bin/env python3
"""
Build and deploy aidp_compat wheel to AIDP cluster.
====================================================
Builds the wheel, uploads via cluster libraries API, and triggers restart.

Usage:
    python3 scripts/deploy_aidp_compat.py --cluster <CLUSTER-ID>
"""

import argparse
import json
import os
import subprocess
import sys
import glob

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# AIDP config — sourced from aidp_migrator.yaml (via aidp_config) with env-var fallback.
from _aidp_env_defaults import load_aidp_defaults, aidp_base_url
_defs = load_aidp_defaults()
DATALAKE_OCID = _defs["lake_ocid"]
WORKSPACE_ID  = _defs["workspace_id"]
OCI_PROFILE   = _defs["oci_profile"]
AIDP_BASE     = aidp_base_url(_defs["region"]) if _defs["region"] else ""


def build_wheel():
    """Build the aidp_compat wheel."""
    print("Building wheel...")
    # Clean old builds
    for d in ["build", "dist", "aidp_compat.egg-info"]:
        path = os.path.join(PROJECT_DIR, d)
        if os.path.exists(path):
            import shutil
            shutil.rmtree(path)

    result = subprocess.run(
        [sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "-w", "dist"],
        cwd=PROJECT_DIR,
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Build failed: {result.stderr}")
        sys.exit(1)

    wheels = glob.glob(os.path.join(PROJECT_DIR, "dist", "aidp_compat-*.whl"))
    if not wheels:
        print("No wheel found in dist/")
        sys.exit(1)

    wheel_path = wheels[0]
    print(f"Built: {wheel_path}")
    return wheel_path


# The CLI-selected YAML; published in main() beside cluster_lifecycle.configure()
# so authentication uses the SAME config that selected the library repository
# (PR #234 Param review: --config B must not auth with a discovered A).
SELECTED_CONFIG_PATH = None


def _publish_selected_config(config_path):
    """Make the CLI-selected YAML authoritative for EVERY signer built in this
    process — not only this module's get_signer. deploy_wheel() also creates an
    AIDPSession, whose connect() builds its own signer via get_oci_signer(profile)
    with no config argument (PR #234 review round 4): without this publication,
    --config B could stage the wheel as a discovered YAML A's identity while the
    library REST calls authenticate as B. $AIDP_CONFIG_PATH is the loader's own
    documented env mechanism; an explicit config_path argument still outranks it.
    """
    global SELECTED_CONFIG_PATH
    SELECTED_CONFIG_PATH = config_path
    if config_path:
        os.environ["AIDP_CONFIG_PATH"] = config_path


def get_signer():
    # Single auth source: oci_auth (Credential Store on-cluster → config file on laptop).
    from oci_auth import get_oci_signer as _shared
    _, signer = _shared(OCI_PROFILE, config_path=SELECTED_CONFIG_PATH)
    return signer


def deploy_wheel(wheel_path: str, cluster_id: str):
    """Deploy wheel to AIDP cluster via session upload + libraries API."""
    import asyncio
    import base64
    import requests

    signer = get_signer()
    wheel_name = os.path.basename(wheel_path)

    with open(wheel_path, "rb") as f:
        wheel_bytes = f.read()

    # Stage into the YAML-configured dependency repository (single source of
    # truth shared with dbx_export + cluster_lifecycle prerequisite checks).
    from cluster_lifecycle import AIDP_LIB_DIR
    workspace_path = f"{AIDP_LIB_DIR.lstrip('/').removeprefix('Workspace/')}/{wheel_name}"

    # Step 1: Upload wheel to /Workspace via cluster session (binary-safe)
    print(f"Uploading {wheel_name} ({len(wheel_bytes)} bytes) via cluster session...")

    from aidp_executor import AIDPSession

    async def _upload():
        session = AIDPSession(
            cluster_id=cluster_id,
            lake_ocid=DATALAKE_OCID,
            workspace_id=WORKSPACE_ID,
            oci_profile=OCI_PROFILE,
        )
        await session.connect()

        b64 = base64.b64encode(wheel_bytes).decode("ascii")
        target = f"{AIDP_LIB_DIR}/{wheel_name}"
        CHUNK = 40000
        chunks = [b64[i:i+CHUNK] for i in range(0, len(b64), CHUNK)]

        # First chunk - create file
        await session._execute_locked(f"""
import base64, builtins, os
path = "{target}"
os.makedirs(os.path.dirname(path), exist_ok=True)
with builtins.open(path, 'wb') as f:
    f.write(base64.b64decode("{chunks[0]}"))
""", timeout=30)

        # Remaining chunks - append
        for chunk in chunks[1:]:
            await session._execute_locked(f"""
import base64, builtins
with builtins.open("{target}", 'ab') as f:
    f.write(base64.b64decode("{chunk}"))
""", timeout=30)

        # Verify
        result = await session._execute_locked(f"""
import os, zipfile
path = "{target}"
size = os.path.getsize(path)
valid = False
try:
    with zipfile.ZipFile(path) as z:
        valid = len(z.namelist()) > 0
except:
    pass
print(f"{{size}} bytes, valid={{valid}}")
""", timeout=15)

        from context_tools import _unwrap_aidp_text
        from aidp_executor import format_outputs
        output = _unwrap_aidp_text(format_outputs(result.get("outputs", [])))
        print(f"  Uploaded: {output.strip()}")

        await session.close()

    asyncio.run(_upload())

    # Step 2: Install via cluster libraries API
    print(f"Installing on cluster {cluster_id}...")
    libs_url = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/clusters/{cluster_id}/libraries"

    # AIDP libraries API contract (matches scripts/cluster_lifecycle.py which
    # already ships working install code). One PATCH per operation is the
    # expected shape — batching multiple items in one PATCH races per
    # "Install cluster libraries sequentially" ops rule.
    workspace_full = workspace_path if workspace_path.startswith("/") else f"/Workspace/{workspace_path}"

    # Uninstall any existing aidp_compat first (by workspace path pattern)
    list_resp = requests.get(libs_url, auth=signer)
    if list_resp.status_code == 200:
        existing = list_resp.json().get("items", [])
        for lib in existing:
            p = lib.get("path", "")
            if "aidp_compat" in p and p.endswith(".whl"):
                un_body = {"items": [{
                    "operation": "UNINSTALL",
                    "type": "WORKSPACE_FILE",
                    "path": p,
                }]}
                r = requests.patch(libs_url, json=un_body, auth=signer,
                                   headers={"Content-Type": "application/json"})
                print(f"  Uninstall {p.rsplit('/', 1)[-1]}: {r.status_code}")

    # Install new version
    install_body = {"items": [{
        "operation": "INSTALL",
        "type": "WORKSPACE_FILE",
        "path": workspace_full,
    }]}
    resp = requests.patch(libs_url, json=install_body, auth=signer,
                          headers={"Content-Type": "application/json"})
    print(f"  Install new: {resp.status_code}")

    if resp.status_code in (200, 202):
        print(f"Deployed {wheel_name} to cluster {cluster_id} (202 = async accepted)")
        print("NOTE: Cluster may need restart for new library to take effect")
    else:
        print(f"Deploy failed: {resp.text[:200]}")


def main():
    parser = argparse.ArgumentParser(description="Build and deploy aidp_compat")
    parser.add_argument("--cluster", required=True,
                        help="AIDP cluster ID (required — no default; must be provided explicitly)")
    parser.add_argument("--build-only", action="store_true", help="Only build, don't deploy")
    parser.add_argument("--config", default=None,
                        help="Path to aidp_migrator.yaml (falls back to "
                             "$AIDP_CONFIG_PATH / default discovery). Resolves "
                             "paths.lib_repository_dir — where the wheel is staged.")
    args = parser.parse_args()

    # Re-resolve the dependency repository from the SAME --config the migrator
    # uses so the wheel lands where the prerequisite check will look for it.
    import cluster_lifecycle
    cluster_lifecycle.configure(args.config)
    _publish_selected_config(args.config)

    wheel_path = build_wheel()

    if not args.build_only:
        deploy_wheel(wheel_path, args.cluster)


if __name__ == "__main__":
    main()
