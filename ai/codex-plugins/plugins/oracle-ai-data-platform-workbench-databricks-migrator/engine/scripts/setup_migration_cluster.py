#!/usr/bin/env python3
"""
Setup Migration Cluster
========================
Deploys migration dependencies to a dedicated AIDP compute cluster:
1. Copies baseline JARs to the configured dependency repository
   (paths.lib_repository_dir, default /Workspace/migration-dependencies/libs/)
2. Writes default_requirements.txt (control-plane deps) and
   target_runtime_requirements.txt (workload-cluster deps)
3. Copies JARs to /aidp/libraries/java/jars/ (Spark classpath)
4. Installs pip packages

Per-job JARs and per-job pip packages are handled elsewhere:
  - Per-job JARs: attached by dbx_export at cluster-provision time (reads the
    source DBX job's cluster spec).
  - Per-job pip: dynamically discovered by cluster_lifecycle.py and appended
    to <job_output_path>/job_requirements.txt at migrate time.

Uses the AIDP executor to run commands on the cluster.

Usage:
    python3 setup_migration_cluster.py [--cluster <id>]
"""

import asyncio
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession, format_outputs

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


with open(os.path.join(PROJECT_DIR, "config", "default_requirements.txt"), encoding="utf-8") as _requirements_file:
    REQUIREMENTS_TXT = _requirements_file.read()
with open(os.path.join(PROJECT_DIR, "config", "target_runtime_requirements.txt"), encoding="utf-8") as _target_requirements_file:
    TARGET_REQUIREMENTS_TXT = _target_requirements_file.read()

# Baseline JARs (generic Spark deps, not customer-specific).
# Source path on workspace -> destination name.
JARS = [
    ("/Workspace/jars/hudi-spark3.5-bundle_2.12-0.15.0-3975ca4.jar", "hudi-spark3.5-bundle_2.12-0.15.0-3975ca4.jar"),
    ("/Workspace/jars/scala-logging_2.12-3.9.5.jar", "scala-logging_2.12-3.9.5.jar"),
]


async def run_step(session, description, code, timeout=300):
    """Run a step and print result."""
    print(f"\n--- {description} ---")
    result = await session.execute(code, timeout=timeout)
    output = format_outputs(result.get("outputs", []))
    status = result.get("status", "error")
    if output:
        # Truncate long output
        display = output[:2000]
        if len(output) > 2000:
            display += f"\n... ({len(output)} chars)"
        print(display)
    if status != "ok":
        print(f"[{status}]")
    return status == "ok"


async def main():
    parser = argparse.ArgumentParser(description="Setup migration cluster")
    parser.add_argument("--cluster", required=True,
                        help="AIDP cluster ID (required — no default)")
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--skip-jars", action="store_true", help="Skip JAR copy step")
    parser.add_argument("--skip-pip", action="store_true", help="Skip pip install step")
    parser.add_argument("--config", default=None,
                        help="Path to aidp_migrator.yaml (falls back to "
                             "$AIDP_CONFIG_PATH / default discovery). Resolves "
                             "paths.lib_repository_dir — where JARs are provisioned.")
    args = parser.parse_args()

    # Provision into the SAME dependency repository the migrator resolves
    # (paths.lib_repository_dir), so setup and runtime never disagree.
    import cluster_lifecycle
    cluster_lifecycle.configure(args.config)
    lib_dir = cluster_lifecycle.AIDP_LIB_DIR

    print("=" * 60)
    print("AIDP Migration Cluster Setup")
    print("=" * 60)
    print(f"Cluster: {args.cluster}")
    print(f"Dependency repository: {lib_dir}")

    session = AIDPSession(cluster_id=args.cluster, oci_profile=args.profile)
    await session.connect()

    try:
        # Step 1: Create migration-dependencies folder structure
        await run_step(session, "Create folder structure", """
import os
os.makedirs('__LIB_DIR__', exist_ok=True)
print('Folder structure ready')
print('Writable:', os.access('/aidp/libraries/java/jars', os.W_OK))
""".replace("__LIB_DIR__", lib_dir))

        # Step 2: Write default_requirements.txt
        await run_step(session, "Write default_requirements.txt", f"""
with open('/Workspace/migration-dependencies/default_requirements.txt', 'w') as f:
    f.write('''{REQUIREMENTS_TXT}''')
print('default_requirements.txt written')
""")

        await run_step(session, "Write target_runtime_requirements.txt", f"""
with open('/Workspace/migration-dependencies/target_runtime_requirements.txt', 'w') as f:
    f.write('''{TARGET_REQUIREMENTS_TXT}''')
print('target_runtime_requirements.txt written')
""")

        # Step 3: Copy JARs one at a time (to avoid timeouts on large files)
        if not args.skip_jars:
            # Copy all JARs in one execution to minimize session overhead,
            # but with sleeps between copies to let the filesystem sync
            await run_step(session, "Copy all JARs to workspace and classpath", """
import shutil, os, time

jars = [
    # Tool-owned baseline artifacts only. Add YOUR deployment's exported JARs
    # here (source workspace path, classpath filename) — the exporter stages
    # them under /Workspace; never assume another deployment's inventory.
    ('/Workspace/jars/hudi-spark3.5-bundle_2.12-0.15.0-3975ca4.jar', 'hudi-spark3.5-bundle_2.12-0.15.0-3975ca4.jar'),
    ('/Workspace/jars/scala-logging_2.12-3.9.5.jar', 'scala-logging_2.12-3.9.5.jar'),
]

ws_dir = '__LIB_DIR__'
cp_dir = '/aidp/libraries/java/jars'

for src, name in jars:
    if not os.path.exists(src):
        print(f'MISSING: {src}')
        continue

    src_size = os.path.getsize(src)

    # Copy to workspace migration-dependencies
    ws_dst = os.path.join(ws_dir, name)
    try:
        shutil.copy2(src, ws_dst)
        print(f'Copied to workspace: {name} ({src_size:,} bytes)')
    except Exception as e:
        print(f'Workspace copy failed for {name}: {e}')

    # Copy to Spark classpath
    cp_dst = os.path.join(cp_dir, name)
    try:
        shutil.copy2(src, cp_dst)
        print(f'Copied to classpath: {name}')
    except Exception as e:
        print(f'Classpath copy failed for {name}: {e}')

    # Let filesystem sync before next copy
    time.sleep(5)

print()
print('All copies issued. Filesystem will sync in the background.')
""".replace("__LIB_DIR__", lib_dir), timeout=600)  # 10 min timeout for all JAR copies

            print("\n  JARs copied. Filesystem needs time to sync.")
            print("  Run with --skip-jars --skip-pip later to verify.")

        # Step 4: Install pip packages
        if not args.skip_pip:
            await run_step(session, "Install pip packages", """
import subprocess, sys
result = subprocess.run(
    [sys.executable, '-m', 'pip', 'install', '-r', '/Workspace/migration-dependencies/default_requirements.txt'],
    capture_output=True, text=True, timeout=600
)
# Show just the install summary
lines = result.stdout.split('\\n')
for line in lines:
    if 'Successfully installed' in line or 'already satisfied' in line.lower() or 'ERROR' in line:
        print(line)
if result.returncode != 0:
    print('STDERR:', result.stderr[-500:])
print(f'Exit code: {result.returncode}')
""", timeout=600)

        # Step 5: Write copy_jars.sh init script
        await run_step(session, "Write copy_jars.sh", """
script = '''#!/bin/bash
DEST=/aidp/libraries/java/jars
SRC=__LIB_DIR__
for jar in "$SRC"/*.jar; do
    name=$(basename "$jar")
    if [ ! -f "$DEST/$name" ]; then
        cp "$jar" "$DEST/$name"
        echo "COPIED: $name"
    fi
done
echo "JARs deployed to Spark classpath."
'''
with open('/Workspace/migration-dependencies/copy_jars.sh', 'w') as f:
    f.write(script)
import os
os.chmod('/Workspace/migration-dependencies/copy_jars.sh', 0o755)
print('copy_jars.sh written and made executable')
""".replace("__LIB_DIR__", lib_dir))

        # Step 6: Test JAR class loading
        await run_step(session, "Test class loading (NOTE: may need cluster restart)", """
tests = [
    ('org.apache.hudi.DataSourceReadOptions', 'Hudi'),
    ('com.typesafe.scalalogging.Logger', 'scala-logging'),
    ('io.delta.tables.DeltaTable', 'Delta Lake'),
]
for class_name, label in tests:
    try:
        cls = sc._jvm.java.lang.Class.forName(class_name)
        print(f'OK: {label}')
    except:
        print(f'NOT LOADED (needs restart): {label}')
""")

        # Final summary
        await run_step(session, "Setup Summary", """
import os
print('=== Migration Cluster Setup Summary ===')
print()

# Workspace deps
ws_jars = [f for f in os.listdir('__LIB_DIR__') if f.endswith('.jar')]
print(f'__LIB_DIR__: {len(ws_jars)} JARs')

# Classpath JARs
cp_jars = [f for f in os.listdir('/aidp/libraries/java/jars') if f.endswith('.jar')]
print(f'/aidp/libraries/java/jars: {cp_jars} JARs')

# requirements.txt
print(f'default_requirements.txt: {os.path.exists("/Workspace/migration-dependencies/default_requirements.txt")}')
print(f'copy_jars.sh: {os.path.exists("/Workspace/migration-dependencies/copy_jars.sh")}')

print()
print('NOTE: If class loading test shows NOT LOADED, the cluster needs to be')
print('restarted for JARs in /aidp/libraries/java/jars/ to take effect.')
print('After restart, run this script again with --skip-jars --skip-pip to verify.')
""".replace("__LIB_DIR__", lib_dir))

    finally:
        await session.close()

    print("\n" + "=" * 60)
    print("Setup complete. If JARs need a restart, restart the cluster and re-run with:")
    print(f"  python3 setup_migration_cluster.py --cluster {args.cluster} --skip-jars --skip-pip")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
