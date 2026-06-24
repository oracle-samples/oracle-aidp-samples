#!/usr/bin/env python3
"""
Build and deploy aidp_compat wheel to AIDP cluster.
====================================================
Builds the wheel, uploads via cluster libraries API, and triggers restart.

Usage:
    python3 scripts/deploy_aidp_compat.py --cluster <CLUSTER_ID>
"""

import argparse
import json
import os
import subprocess
import sys
import glob

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AIDP_BASE = "https://aidp.<OCI_REGION>.oci.oraclecloud.com/20240831"
DATALAKE_OCID = "<DATALAKE_OCID>"
WORKSPACE_ID = "<WORKSPACE_ID>"
OCI_PROFILE = "CUSTOMER"
DEFAULT_CLUSTER = "<CLUSTER_ID>"


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


def get_signer():
    import oci
    config = oci.config.from_file(profile_name=OCI_PROFILE)
    return oci.signer.Signer(
        tenancy=config["tenancy"], user=config["user"],
        fingerprint=config["fingerprint"],
        private_key_file_location=config["key_file"],
    )


def deploy_wheel(wheel_path: str, cluster_id: str):
    """Deploy wheel to AIDP cluster via session upload + libraries API."""
    import asyncio
    import base64
    import requests

    signer = get_signer()
    wheel_name = os.path.basename(wheel_path)

    with open(wheel_path, "rb") as f:
        wheel_bytes = f.read()

    workspace_path = f"migration-dependencies/{wheel_name}"

    # Step 1: Upload wheel to /Workspace via cluster session (binary-safe)
    print(f"Uploading {wheel_name} ({len(wheel_bytes)} bytes) via cluster session...")

    from aidp_executor import AIDPSession

    async def _upload():
        session = AIDPSession(cluster_id=cluster_id)
        await session.connect()

        b64 = base64.b64encode(wheel_bytes).decode("ascii")
        target = f"/Workspace/migration-dependencies/{wheel_name}"
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

    # First uninstall old version
    uninstall_body = json.dumps({
        "uninstallLibraries": [{
            "pypi": {"package": "aidp_compat"}
        }]
    })
    resp = requests.patch(libs_url, data=uninstall_body, auth=signer,
                          headers={"Content-Type": "application/json"})
    print(f"  Uninstall old: {resp.status_code}")

    # Install new version
    install_body = json.dumps({
        "installLibraries": [{
            "whl": f"/Workspace/{workspace_path}"
        }]
    })
    resp = requests.patch(libs_url, data=install_body, auth=signer,
                          headers={"Content-Type": "application/json"})
    print(f"  Install new: {resp.status_code}")

    if resp.status_code in (200, 202):
        print(f"Deployed {wheel_name} to cluster {cluster_id} (202 = async accepted)")
        print("NOTE: Cluster may need restart for new library to take effect")
    else:
        print(f"Deploy failed: {resp.text[:200]}")


def main():
    parser = argparse.ArgumentParser(description="Build and deploy aidp_compat")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER)
    parser.add_argument("--build-only", action="store_true", help="Only build, don't deploy")
    args = parser.parse_args()

    wheel_path = build_wheel()

    if not args.build_only:
        deploy_wheel(wheel_path, args.cluster)


if __name__ == "__main__":
    main()
