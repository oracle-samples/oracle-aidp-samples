#!/usr/bin/env python3
"""
Setup Migration Cluster
========================
Deploys migration dependencies to a dedicated AIDP compute cluster:
1. Copies JARs to /Workspace/migration-dependencies/jars/ (one at a time for large files)
2. Writes requirements.txt
3. Copies JARs to /aidp/libraries/java/jars/ (Spark classpath)
4. Installs pip packages

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

# New migration testing cluster
DEFAULT_CLUSTER = "<CLUSTER_ID>"

REQUIREMENTS_TXT = """pandas>=2.3.0
numpy>=2.4.0
requests>=2.32.0
oci>=2.167.0
nbformat>=5.10.0
ray>=2.54.0
slack_sdk>=3.27.0
boto3>=1.42.0
IPython>=8.0.0
matplotlib>=3.7.0
scikit-learn>=1.3.0
scipy>=1.11.0
xgboost>=1.7.6
seaborn>=0.12.0
shap>=0.42.0
optuna>=3.3.0
joblib>=1.3.0
pytz>=2023.3
python-dateutil>=2.8.0
python-box>=7.0.0
tqdm>=4.65.0
pyarrow>=12.0.0
openpyxl>=3.1.0
xlsxwriter>=3.1.0
fuzzywuzzy>=0.18.0
python-Levenshtein>=0.21.0
brotli>=1.0.9
mlflow>=2.8.0
plotly>=5.15.0
kaleido>=0.2.1
dataframe-image>=0.2.0
Pillow>=10.0.0
pyathena>=3.0.0
trino>=0.326.0
gspread>=5.10.0
gspread-dataframe>=3.3.0
econml>=0.14.0
psutil>=5.9.0
omegaconf>=2.3.0"""

# JARs: source path on workspace -> destination name
JARS = [
    ("/Workspace/jars/hudi-spark3.5-bundle_2.12-0.15.0.jar", "hudi-spark3.5-bundle_2.12-0.15.0.jar"),
    ("/Workspace/feature_lib_assembly.jar", "feature_lib_assembly.jar"),
    ("/Workspace/<customer_jars>/featurelib2_assembly.jar", "featurelib2_assembly.jar"),
    ("/Workspace/<customer_jars>/feature_lib3_assembly.jar", "feature_lib3_assembly.jar"),
    ("/Workspace/message_parser_assembly.jar", "message_parser_assembly.jar"),
    ("/Workspace/<production_jars>/decryptor_assembly.jar", "decryptor_assembly.jar"),
    ("/Workspace/<production_jars>/decrypt_udf.jar", "Spark_Decrypt_UDF_1.0.2_Final.jar"),
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
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER)
    parser.add_argument("--profile", default="CUSTOMER")
    parser.add_argument("--skip-jars", action="store_true", help="Skip JAR copy step")
    parser.add_argument("--skip-pip", action="store_true", help="Skip pip install step")
    args = parser.parse_args()

    print("=" * 60)
    print("AIDP Migration Cluster Setup")
    print("=" * 60)
    print(f"Cluster: {args.cluster}")

    session = AIDPSession(cluster_id=args.cluster, oci_profile=args.profile)
    await session.connect()

    try:
        # Step 1: Create migration-dependencies folder structure
        await run_step(session, "Create folder structure", """
import os
os.makedirs('/Workspace/migration-dependencies/jars', exist_ok=True)
print('Folder structure ready')
print('Writable:', os.access('/aidp/libraries/java/jars', os.W_OK))
""")

        # Step 2: Write requirements.txt
        await run_step(session, "Write requirements.txt", f"""
with open('/Workspace/migration-dependencies/requirements.txt', 'w') as f:
    f.write('''{REQUIREMENTS_TXT}''')
print('requirements.txt written')
""")

        # Step 3: Copy JARs one at a time (to avoid timeouts on large files)
        if not args.skip_jars:
            # Copy all JARs in one execution to minimize session overhead,
            # but with sleeps between copies to let the filesystem sync
            await run_step(session, "Copy all JARs to workspace and classpath", """
import shutil, os, time

jars = [
    ('/Workspace/jars/hudi-spark3.5-bundle_2.12-0.15.0.jar', 'hudi-spark3.5-bundle_2.12-0.15.0.jar'),
    ('/Workspace/feature_lib_assembly.jar', 'feature_lib_assembly.jar'),
    ('/Workspace/<customer_jars>/featurelib2_assembly.jar', 'featurelib2_assembly.jar'),
    ('/Workspace/<customer_jars>/feature_lib3_assembly.jar', 'feature_lib3_assembly.jar'),
    ('/Workspace/message_parser_assembly.jar', 'message_parser_assembly.jar'),
    ('/Workspace/<production_jars>/decryptor_assembly.jar', 'decryptor_assembly.jar'),
    ('/Workspace/<production_jars>/decrypt_udf.jar', 'Spark_Decrypt_UDF_1.0.2_Final.jar'),
    ('/Workspace/jars/scala-logging_2.12-3.9.5.jar', 'scala-logging_2.12-3.9.5.jar'),
]

ws_dir = '/Workspace/migration-dependencies/jars'
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
""", timeout=600)  # 10 min timeout for all JAR copies

            print("\n  JARs copied. Filesystem needs time to sync.")
            print("  Run with --skip-jars --skip-pip later to verify.")

        # Step 4: Install pip packages
        if not args.skip_pip:
            await run_step(session, "Install pip packages", """
import subprocess, sys
result = subprocess.run(
    [sys.executable, '-m', 'pip', 'install', '-r', '/Workspace/migration-dependencies/requirements.txt'],
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
SRC=/Workspace/migration-dependencies/jars
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
""")

        # Step 6: Test JAR class loading
        await run_step(session, "Test class loading (NOTE: may need cluster restart)", """
tests = [
    ('org.apache.hudi.DataSourceReadOptions', 'Hudi'),
    ('com.customer.datascience.feature_lib.Constants', 'FeatureLib'),
    ('com.customer.datascience.feature_lib2.ConcurrentFLHandler', 'FeatureLib2'),
    ('com.customer.util_a.decryptor.Decryptor', 'Decryptor'),
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
ws_jars = [f for f in os.listdir('/Workspace/migration-dependencies/jars') if f.endswith('.jar')]
print(f'/Workspace/migration-dependencies/jars: {len(ws_jars)} JARs')

# Classpath JARs
cp_jars = [f for f in os.listdir('/aidp/libraries/java/jars') if f.endswith('.jar')]
print(f'/aidp/libraries/java/jars: {cp_jars} JARs')

# requirements.txt
print(f'requirements.txt: {os.path.exists("/Workspace/migration-dependencies/requirements.txt")}')
print(f'copy_jars.sh: {os.path.exists("/Workspace/migration-dependencies/copy_jars.sh")}')

print()
print('NOTE: If class loading test shows NOT LOADED, the cluster needs to be')
print('restarted for JARs in /aidp/libraries/java/jars/ to take effect.')
print('After restart, run this script again with --skip-jars --skip-pip to verify.')
""")

    finally:
        await session.close()

    print("\n" + "=" * 60)
    print("Setup complete. If JARs need a restart, restart the cluster and re-run with:")
    print(f"  python3 setup_migration_cluster.py --cluster {args.cluster} --skip-jars --skip-pip")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
