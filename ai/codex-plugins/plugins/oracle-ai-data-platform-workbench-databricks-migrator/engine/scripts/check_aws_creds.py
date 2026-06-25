#!/usr/bin/env python3
"""Search AIDP workspace for AWS credentials."""
import sys, asyncio, json, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession

def unwrap(outputs):
    text = ""
    for o in outputs:
        if o.get("type") == "stream":
            raw = o.get("text", "")
            try:
                items = json.loads(raw)
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict) and "value" in item:
                            text += item["value"]
                    continue
            except:
                pass
            text += raw
        elif o.get("type") == "error":
            text += "ERROR: " + o.get("evalue", "") + "\n"
    return text

async def main():
    session = AIDPSession(cluster_id="<your-cluster-id>")
    await session.connect()

    cells = [
        # 1: Env vars
        'import os\nprint("=== AWS env vars ===")\nfor k, v in sorted(os.environ.items()):\n    kup = k.upper()\n    if "AWS" in kup or "SECRET" in kup or "ENCRYPT" in kup or "DECRYPT" in kup:\n        print(f"  {k}={v[:80]}")',

        # 2: Spark configs
        'print("=== AWS Spark configs ===")\nkeys = ["spark.hadoop.fs.s3a.access.key", "spark.hadoop.fs.s3a.secret.key", "spark.hadoop.fs.s3a.endpoint", "spark.hadoop.fs.s3a.aws.credentials.provider"]\nfor key in keys:\n    try:\n        val = spark.conf.get(key)\n        print(f"  {key}={val[:80]}")\n    except:\n        pass',

        # 3: Find credential files
        'import os\nprint("=== Credential-related files ===")\nfor root, dirs, files in os.walk("/Workspace"):\n    depth = root.count("/") - 1\n    if depth > 4:\n        dirs.clear()\n        continue\n    for f in files:\n        fl = f.lower()\n        if any(k in fl for k in ["credential", "aws", ".env", "decrypt", "encrypt", "secret"]):\n            full = os.path.join(root, f)\n            print(f"  {full} ({os.path.getsize(full)} bytes)")',

        # 4: Config/properties files
        'import os\nprint("=== Config files ===")\nfor root, dirs, files in os.walk("/Workspace"):\n    depth = root.count("/") - 1\n    if depth > 4:\n        dirs.clear()\n        continue\n    for f in files:\n        if f.endswith((".properties", ".conf", ".cfg", ".ini")):\n            full = os.path.join(root, f)\n            print(f"  {full}")',

        # 5: Decrypt JAR internals
        'import subprocess\nprint("=== Encryption-related JARs ===")\nresult = subprocess.run(["find", "/aidp/libraries", "-name", "*decrypt*", "-o", "-name", "*encrypt*", "-o", "-name", "*secret*"], capture_output=True, text=True, timeout=10)\nfor line in result.stdout.strip().split("\\n"):\n    if line.strip() and line.endswith(".jar"):\n        print(f"JAR: {line}")\n        r2 = subprocess.run(["jar", "tf", line], capture_output=True, text=True, timeout=10)\n        for entry in r2.stdout.strip().split("\\n"):\n            el = entry.lower()\n            if any(k in el for k in ["config", "properties", "secret", "aws", "application", "reference", "encrypt", "decrypt"]):\n                print(f"  {entry}")',

        # 6: Init scripts with AWS refs
        'import os\nprint("=== Init scripts with AWS refs ===")\nfor root, dirs, files in os.walk("/Workspace"):\n    depth = root.count("/") - 1\n    if depth > 4:\n        dirs.clear()\n        continue\n    for f in files:\n        if f.endswith(".sh"):\n            full = os.path.join(root, f)\n            try:\n                with open(full) as fh:\n                    content = fh.read()\n                if "AWS" in content or "aws_" in content or "secret" in content.lower():\n                    print(f"  {full}")\n                    for line in content.split("\\n"):\n                        ll = line.lower()\n                        if "aws" in ll or "secret" in ll or "key" in ll:\n                            print(f"    {line.strip()[:120]}")\n            except:\n                pass',

        # 7: Check Hadoop configs for AWS
        'print("=== Hadoop AWS configs ===")\nhc = spark.sparkContext._jsc.hadoopConfiguration()\nfor key in ["fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.endpoint", "fs.s3a.aws.credentials.provider", "fs.s3a.assumed.role.arn"]:\n    val = hc.get(key)\n    if val:\n        print(f"  {key}={val[:80]}")',

        # 8: Check the decrypt JAR class for how it gets keys
        'import subprocess\nprint("=== Inspect encryption-related classes ===")\nimport glob\njars = glob.glob("/aidp/libraries/java/jars/*decrypt*") + glob.glob("/aidp/libraries/java/jars/*encrypt*") + glob.glob("/aidp/libraries/java/jars/*secret*")\nfor jar in jars[:3]:\n    print(f"JAR: {jar}")\n    r = subprocess.run(["jar", "tf", jar], capture_output=True, text=True, timeout=10)\n    classes = [e for e in r.stdout.strip().split("\\n") if "decrypt" in e.lower() or "encrypt" in e.lower() or "secret" in e.lower() or "config" in e.lower()]\n    for c in classes[:20]:\n        print(f"  {c}")',
    ]

    for i, code in enumerate(cells):
        print(f"\n--- Cell {i+1} ---")
        result = await session.execute(code, timeout=30)
        print(unwrap(result.get("outputs", [])))

    await session.close()

asyncio.run(main())
