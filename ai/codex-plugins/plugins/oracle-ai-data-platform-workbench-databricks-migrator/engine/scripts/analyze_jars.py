#!/usr/bin/env python3
"""
JAR Dependency Analyzer
========================
Downloads JARs from the AIDP workspace and analyzes them for Spark 3.5 compatibility.
Checks: Scala version, Spark version compiled against, class availability.
"""

import json
import os
import sys
import subprocess
import zipfile
import re
import time
import oci
import requests
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JARS_DIR = os.path.join(PROJECT_DIR, "jars")
REPORTS_DIR = os.path.join(PROJECT_DIR, "reports")

# AIDP config — sourced from aidp_migrator.yaml (via aidp_config) with env-var fallback.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _aidp_env_defaults import load_aidp_defaults, aidp_base_url
_defs = load_aidp_defaults()
DATALAKE_OCID = _defs["lake_ocid"]
WORKSPACE_ID  = _defs["workspace_id"]
OCI_PROFILE   = _defs["oci_profile"]
AIDP_BASE     = aidp_base_url(_defs["region"]) if _defs["region"] else ""
DOWNLOAD_META_URL = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/actions/downloadFileMeta"

# Known JARs on the workspace (from enumeration)
WORKSPACE_JARS: list = [
    # Populate with your workspace's JAR paths (e.g. from the dbx_export
    # manifest or 'Databricks workspace / Production Jar' folder listing).
    # Shipped empty: the tool must never assume one deployment's inventory.
]


def get_oci_signer():
    # Single auth source: oci_auth (Credential Store on-cluster → config file on laptop).
    from oci_auth import get_oci_signer as _shared
    _, signer = _shared(OCI_PROFILE)
    return signer


def download_jar(signer, jar_path: str, local_dir: str) -> str:
    """Download a JAR from AIDP workspace."""
    headers = {"Content-Type": "application/json", "path": jar_path, "type": "FILE"}
    resp = requests.post(DOWNLOAD_META_URL, auth=signer, headers=headers, data="")
    resp.raise_for_status()
    par_url = resp.json().get("parUrl")
    if not par_url:
        raise Exception("No parUrl returned")

    local_path = os.path.join(local_dir, os.path.basename(jar_path))
    resp = requests.get(par_url)
    resp.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(resp.content)
    return local_path


def analyze_jar(jar_path: str) -> dict:
    """Analyze a JAR file for Spark/Scala compatibility."""
    result = {
        "jar_name": os.path.basename(jar_path),
        "jar_path": jar_path,
        "size_bytes": os.path.getsize(jar_path),
        "scala_version": None,
        "spark_version": None,
        "is_assembly": "assembly" in jar_path.lower() or "all" in jar_path.lower(),
        "classes": [],
        "examplecorp_packages": [],
        "spark_deps": [],
        "potential_conflicts": [],
        "compatibility": "UNKNOWN",
    }

    try:
        with zipfile.ZipFile(jar_path, "r") as zf:
            names = zf.namelist()

            # Count classes
            class_files = [n for n in names if n.endswith(".class")]
            result["class_count"] = len(class_files)

            # Check for Scala version from class files
            scala_classes = [n for n in class_files if "scala" in n.lower()]
            if any("scala/collection/immutable" in n for n in names):
                result["has_scala_stdlib"] = True

            # Look for the pilot deployment-specific packages
            examplecorp_classes = [n for n in class_files if "com/example/" in n]
            examplecorp_packages = set()
            for c in examplecorp_classes:
                parts = c.split("/")
                if len(parts) >= 4:
                    examplecorp_packages.add("/".join(parts[:4]))
            result["examplecorp_packages"] = sorted(examplecorp_packages)

            # Check for Spark API usage
            spark_classes = [n for n in class_files if "org/apache/spark" in n]
            result["bundles_spark"] = len(spark_classes) > 0
            if spark_classes:
                result["spark_deps"].append(f"{len(spark_classes)} Spark classes bundled")

            # Check for Hudi
            hudi_classes = [n for n in class_files if "org/apache/hudi" in n]
            if hudi_classes:
                result["bundles_hudi"] = True
                result["spark_deps"].append(f"{len(hudi_classes)} Hudi classes")

            # Check for Delta
            delta_classes = [n for n in class_files if "io/delta" in n]
            if delta_classes:
                result["bundles_delta"] = True
                result["spark_deps"].append(f"{len(delta_classes)} Delta classes")

            # Check MANIFEST.MF for version info
            if "META-INF/MANIFEST.MF" in names:
                with zf.open("META-INF/MANIFEST.MF") as mf:
                    manifest = mf.read().decode("utf-8", errors="replace")
                    result["manifest_preview"] = manifest[:500]

                    # Look for Scala version
                    scala_match = re.search(r"Scala[_-]?[Vv]ersion[:\s]+(\S+)", manifest)
                    if scala_match:
                        result["scala_version"] = scala_match.group(1)

                    # Look for Spark version
                    spark_match = re.search(r"Spark[_-]?[Vv]ersion[:\s]+(\S+)", manifest)
                    if spark_match:
                        result["spark_version"] = spark_match.group(1)

            # Infer Scala version from JAR name
            if "_2.12" in jar_path or "_2.12" in os.path.basename(jar_path):
                result["scala_version"] = result["scala_version"] or "2.12 (from filename)"
            elif "_2.13" in jar_path or "_2.13" in os.path.basename(jar_path):
                result["scala_version"] = result["scala_version"] or "2.13 (from filename)"
                result["potential_conflicts"].append(
                    "Scala 2.13 JAR may not be compatible with Spark 3.5 (which uses Scala 2.12)"
                )

            # Check for POM properties
            pom_files = [n for n in names if n.endswith("pom.properties")]
            for pom in pom_files[:5]:
                with zf.open(pom) as pf:
                    props = pf.read().decode("utf-8", errors="replace")
                    if "spark" in props.lower():
                        result["spark_deps"].append(f"POM: {props[:200]}")

            # Compatibility assessment
            conflicts = []
            if result.get("scala_version") and "2.13" in str(result["scala_version"]):
                conflicts.append("Scala 2.13 incompatible with Spark 3.5 (Scala 2.12)")
            if result.get("bundles_spark") and not result.get("is_assembly"):
                conflicts.append("Bundles Spark classes - may conflict with cluster Spark")

            if conflicts:
                result["compatibility"] = "INCOMPATIBLE"
                result["potential_conflicts"].extend(conflicts)
            elif result.get("examplecorp_packages"):
                result["compatibility"] = "NEEDS_TESTING"
            else:
                result["compatibility"] = "LIKELY_OK"

    except zipfile.BadZipFile:
        result["error"] = "Not a valid ZIP/JAR file"
        result["compatibility"] = "ERROR"
    except Exception as e:
        result["error"] = str(e)
        result["compatibility"] = "ERROR"

    return result


def main():
    os.makedirs(JARS_DIR, exist_ok=True)

    print("JAR Dependency Analyzer")
    print("=" * 60)

    signer = get_oci_signer()

    # Download JARs
    print(f"\nDownloading {len(WORKSPACE_JARS)} JARs from AIDP workspace...")
    downloaded = []
    for jar_path in WORKSPACE_JARS:
        local = os.path.join(JARS_DIR, os.path.basename(jar_path))
        if os.path.exists(local) and os.path.getsize(local) > 0:
            print(f"  SKIP (exists): {os.path.basename(jar_path)}")
            downloaded.append(local)
            continue

        print(f"  Downloading: {jar_path}...", end=" ", flush=True)
        try:
            local = download_jar(signer, jar_path, JARS_DIR)
            size = os.path.getsize(local)
            print(f"OK ({size:,} bytes)")
            downloaded.append(local)
            time.sleep(0.2)
        except Exception as e:
            print(f"FAILED: {e}")

    # Analyze JARs
    print(f"\nAnalyzing {len(downloaded)} JARs...")
    results = []
    for jar_path in downloaded:
        print(f"  Analyzing: {os.path.basename(jar_path)}...", end=" ", flush=True)
        analysis = analyze_jar(jar_path)
        results.append(analysis)
        compat = analysis["compatibility"]
        classes = analysis.get("class_count", 0)
        examplecorp = len(analysis.get("examplecorp_packages", []))
        print(f"[{compat}] {classes} classes, {examplecorp} the pilot deployment packages")

        if analysis.get("potential_conflicts"):
            for c in analysis["potential_conflicts"]:
                print(f"    WARNING: {c}")

    # Save report
    report_path = os.path.join(REPORTS_DIR, "jar_analysis.json")
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)

    # Summary
    print(f"\n{'=' * 60}")
    print("JAR ANALYSIS SUMMARY")
    compat_counts = {}
    for r in results:
        c = r["compatibility"]
        compat_counts[c] = compat_counts.get(c, 0) + 1
    for c, count in sorted(compat_counts.items()):
        print(f"  {c}: {count}")

    incompatible = [r for r in results if r["compatibility"] == "INCOMPATIBLE"]
    if incompatible:
        print("\nINCOMPATIBLE JARs:")
        for r in incompatible:
            print(f"  {r['jar_name']}: {', '.join(r['potential_conflicts'])}")

    print(f"\nReport saved to: {report_path}")


if __name__ == "__main__":
    main()
