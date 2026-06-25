"""
AIDP FUSE Dependency Scanner
=============================
Standalone pre-migration scanner that identifies third-party package FUSE risks
in notebook files. Run locally before migration — no cluster required.

FUSE mount problem:
    /Volumes/ paths are backed by OCI Object Storage via FUSE. Packages that
    do internal file I/O (SQLite WAL, memory-mapped files, multi-step writes)
    will hit FUSE issues that are invisible in the notebook source code.

Usage:
    python3 $HOME/.aidp-migrator/engine/scripts/fuse_scanner.py --manifest reports/example_job_manifest.json
    python3 $HOME/.aidp-migrator/engine/scripts/fuse_scanner.py --manifest reports/example_job_manifest.json --output /tmp/fuse_report.md
    python3 $HOME/.aidp-migrator/engine/scripts/fuse_scanner.py --notebook /Volumes/.../my_notebook.ipynb
    python3 $HOME/.aidp-migrator/engine/scripts/fuse_scanner.py --manifest reports/example_job_manifest.json --unknown-packages
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Built-in FUSE risk database
# Extended at runtime from $HOME/.aidp-migrator/engine/scripts/fuse_risk_db.json if present
# ---------------------------------------------------------------------------

FUSE_RISKY_PACKAGES: dict[str, dict] = {
    "joblib": {
        "risk": "HIGH",
        "reason": "joblib.dump() uses memory-mapped files; joblib.Memory uses mmap cache directories — both break on /Volumes/ FUSE.",
        "patterns": [
            r"joblib\.dump\s*\(",
            r"joblib\.load\s*\(",
            r"Memory\s*\(\s*location\s*=",
            r"Parallel\s*\(",  # may write temp files
        ],
        "mitigation": "Replace joblib.dump/load with safe_joblib_dump/safe_joblib_load from aidp_compat.",
        "import_names": ["joblib"],
    },
    "optuna": {
        "risk": "HIGH",
        "reason": "optuna SQLite storage uses WAL mode which requires atomic FUSE ops. JournalFileBackend also uses FUSE-unsafe locking.",
        "patterns": [
            r"create_study\s*\(",
            r"sqlite:///",
            r"JournalFileBackend\s*\(",
            r"RDBStorage\s*\(",
        ],
        "mitigation": "Replace optuna.create_study() with safe_optuna_create_study() + finalize_optuna_study() from aidp_compat.",
        "import_names": ["optuna"],
    },
    "mlflow": {
        "risk": "MEDIUM",
        "reason": "mlflow.log_artifact() and artifact stores write to /Volumes/ by default. Databricks tracking URIs (azuredatabricks://...) won't resolve on AIDP.",
        "patterns": [
            r"mlflow\.log_artifact\s*\(",
            r"mlflow\.set_tracking_uri\s*\(",
            r"mlflow\.set_experiment\s*\(",
            r"/Users/\S+@customer\.com/",  # Databricks user-namespace experiment paths
            r"azuredatabricks://",
            r"databricks://",
        ],
        "mitigation": "Re-point tracking URI: mlflow.set_tracking_uri('file:///tmp/mlruns'). Flatten experiment name. Avoid /Volumes/ artifact store.",
        "import_names": ["mlflow"],
    },
    "h5py": {
        "risk": "HIGH",
        "reason": "h5py uses HDF5 which relies on POSIX file locking and byte-range locks — both broken by FUSE.",
        "patterns": [
            r"h5py\.File\s*\(",
        ],
        "mitigation": "Write HDF5 to /tmp/, then shutil.copy2() to /Volumes/ after close. Use with h5py.File('/tmp/...') as f: pattern.",
        "import_names": ["h5py"],
    },
    "torch": {
        "risk": "HIGH",
        "reason": "torch.save() uses pickle + POSIX rename which can race on FUSE. torch.load() on /Volumes/ may get stale cached inode.",
        "patterns": [
            r"torch\.save\s*\(",
            r"torch\.load\s*\(",
            r"\.save_pretrained\s*\(",
            r"\.from_pretrained\s*\(",
        ],
        "mitigation": "torch.save() to /tmp/ then shutil.copy2() to /Volumes/ + time.sleep(3). For load: time.sleep(3) before torch.load().",
        "import_names": ["torch", "transformers"],
    },
    "tensorflow": {
        "risk": "HIGH",
        "reason": "tf.saved_model.save() writes multiple files (variables/, assets/, saved_model.pb). FUSE may not atomically complete all writes before RestoreV2 reads.",
        "patterns": [
            r"model\.save\s*\(",
            r"tf\.saved_model\.save\s*\(",
            r"tf\.keras\.models\.save_model\s*\(",
            r"tf\.saved_model\.load\s*\(",
            r"keras\.models\.load_model\s*\(",
            r"load_model\s*\(",
        ],
        "mitigation": "Use load_saved_model_from_volumes() from aidp_compat (copies to /tmp then loads locally).",
        "import_names": ["tensorflow", "tf", "keras"],
    },
    "lightgbm": {
        "risk": "MEDIUM",
        "reason": "lgb.Booster(model_file=) reads immediately after save; FUSE cache invalidation may not propagate in time.",
        "patterns": [
            r"model\.save_model\s*\(",
            r"lgb\.Booster\s*\(\s*model_file\s*=",
            r"lgb\.train\s*\(",
        ],
        "mitigation": "Add time.sleep(3) before lgb.Booster(model_file=...) load. Or save to /tmp/ first.",
        "import_names": ["lightgbm", "lgb"],
    },
    "xgboost": {
        "risk": "MEDIUM",
        "reason": "xgb.Booster.save_model() then load_model() on /Volumes/ hits FUSE read-after-write race.",
        "patterns": [
            r"model\.save_model\s*\(",
            r"xgb\.Booster\s*\(\s*model_file\s*=",
            r"\.load_model\s*\(",
        ],
        "mitigation": "Add time.sleep(3) before load_model(). Or save to /tmp/ + copy to /Volumes/ after.",
        "import_names": ["xgboost", "xgb"],
    },
    "catboost": {
        "risk": "MEDIUM",
        "reason": "CatBoost model.save_model() + load_model() on /Volumes/ hits same FUSE read-after-write race as XGBoost.",
        "patterns": [
            r"model\.save_model\s*\(",
            r"CatBoost.*model_file\s*=",
            r"\.load_model\s*\(",
        ],
        "mitigation": "Add time.sleep(3) before load. Or save to /tmp/ + copy to /Volumes/ after.",
        "import_names": ["catboost"],
    },
    "sqlite3": {
        "risk": "HIGH",
        "reason": "SQLite WAL mode requires atomic file ops that FUSE cannot guarantee. Any sqlite3.connect() to a /Volumes/ path will corrupt.",
        "patterns": [
            r"sqlite3\.connect\s*\(",
        ],
        "mitigation": "Use /tmp/ for sqlite3.connect(). If persistence needed: copy .db to /Volumes/ after connection.close().",
        "import_names": ["sqlite3"],
    },
    "dill": {
        "risk": "HIGH",
        "reason": "dill.dump() has same FUSE rename atomicity issues as pickle. Often used for lambda/closure serialization.",
        "patterns": [
            r"dill\.dump\s*\(",
            r"dill\.load\s*\(",
        ],
        "mitigation": "Replace with safe_pickle_dump/safe_pickle_load from aidp_compat (handles any pickle-compatible serializer).",
        "import_names": ["dill"],
    },
    "shelve": {
        "risk": "HIGH",
        "reason": "shelve.open() creates multiple files (dir/bak/dat) with non-atomic multi-file writes — guaranteed to fail on FUSE.",
        "patterns": [
            r"shelve\.open\s*\(",
        ],
        "mitigation": "Avoid shelve on /Volumes/. Use pickle + safe_pickle_dump/load or a database in /tmp/.",
        "import_names": ["shelve"],
    },
    "sklearn": {
        "risk": "LOW",
        "reason": "sklearn itself is safe, but joblib-backed estimator.fit() with memory= caching writes to FUSE.",
        "patterns": [
            r"memory\s*=\s*['\"]?/Volumes/",
            r"Memory\s*\(\s*location\s*=\s*['\"]?/Volumes/",
        ],
        "mitigation": "Set memory= to a /tmp/ path for caching: sklearn.pipeline.Pipeline(..., memory='/tmp/sklearn_cache')",
        "import_names": ["sklearn", "scikit_learn"],
    },
}

# Regex to extract top-level package imports from cell source
_IMPORT_RE = re.compile(
    r"^(?:import\s+([\w.]+)|from\s+([\w.]+)\s+import)",
    re.MULTILINE,
)

# Regex to detect /Volumes/ paths in cell source (confirms FUSE exposure)
_VOLUMES_RE = re.compile(r"/Volumes/")


def _load_external_db(db_path: str) -> None:
    """Load additional risk entries from fuse_risk_db.json (if exists)."""
    if not os.path.exists(db_path):
        return
    try:
        with open(db_path) as f:
            extra = json.load(f)
        for pkg, info in extra.items():
            if pkg not in FUSE_RISKY_PACKAGES:
                FUSE_RISKY_PACKAGES[pkg] = info
    except Exception as e:
        print(f"[fuse_scanner] Warning: could not load {db_path}: {e}", file=sys.stderr)


def extract_imports(src: str) -> list[str]:
    """Return list of top-level package names imported in cell source."""
    pkgs = set()
    for m in _IMPORT_RE.finditer(src):
        raw = m.group(1) or m.group(2)
        pkgs.add(raw.split(".")[0])
    return sorted(pkgs)


def _build_import_index() -> dict[str, str]:
    """Map each import_name alias → canonical package key."""
    index: dict[str, str] = {}
    for pkg_key, info in FUSE_RISKY_PACKAGES.items():
        for alias in info.get("import_names", [pkg_key]):
            index[alias] = pkg_key
    return index


def scan_cell(src: str, import_index: dict[str, str]) -> list[dict]:
    """
    Scan a single cell source for FUSE risks.

    Returns list of findings: [{package, risk, pattern_matched, mitigation, volumes_path_present}]
    """
    findings = []
    imports = extract_imports(src)
    volumes_present = bool(_VOLUMES_RE.search(src))

    seen_packages: set[str] = set()
    for imp in imports:
        pkg_key = import_index.get(imp)
        if not pkg_key or pkg_key in seen_packages:
            continue
        seen_packages.add(pkg_key)

        pkg_info = FUSE_RISKY_PACKAGES[pkg_key]
        matched_patterns = []
        for pat in pkg_info.get("patterns", []):
            if re.search(pat, src):
                matched_patterns.append(pat)

        if matched_patterns:
            findings.append({
                "package": pkg_key,
                "risk": pkg_info["risk"],
                "reason": pkg_info["reason"],
                "patterns_matched": matched_patterns,
                "mitigation": pkg_info["mitigation"],
                "volumes_path_present": volumes_present,
            })

    return findings


def scan_notebook(nb_path: str, import_index: dict[str, str]) -> dict:
    """
    Scan a single .ipynb file for FUSE risks across all cells.

    Returns {path, total_cells, risky_cells, findings_by_cell, unknown_packages}
    """
    try:
        with open(nb_path) as f:
            nb = json.load(f)
    except Exception as e:
        return {"path": nb_path, "error": str(e)}

    cells = nb.get("cells", [])
    risky_cells = []
    all_imports: set[str] = set()
    unknown_pkgs: set[str] = set()

    for cell_idx, cell in enumerate(cells):
        if cell.get("cell_type") != "code":
            continue
        src_lines = cell.get("source", [])
        src = "".join(src_lines) if isinstance(src_lines, list) else src_lines

        cell_imports = extract_imports(src)
        all_imports.update(cell_imports)

        findings = scan_cell(src, import_index)
        if findings:
            risky_cells.append({"cell_idx": cell_idx, "findings": findings})

        # Collect imports not in our database
        for imp in cell_imports:
            if imp and imp not in import_index and imp not in {
                # stdlib / ubiquitous packages that are not FUSE-risky
                "os", "sys", "re", "json", "time", "math", "copy", "io",
                "datetime", "collections", "itertools", "functools", "pathlib",
                "typing", "abc", "enum", "logging", "warnings", "traceback",
                "threading", "multiprocessing", "subprocess", "shutil", "tempfile",
                "hashlib", "base64", "struct", "string", "random", "uuid",
                "numpy", "pandas", "pyspark", "matplotlib", "seaborn", "scipy",
                "requests", "urllib", "http", "ssl", "socket", "email",
                "boto3", "botocore", "oci",  # handled separately
                "aidp_compat", "dbutils",
                "builtins", "__future__",
                "pprint", "textwrap", "decimal", "fractions",
                "operator", "contextlib", "dataclasses", "types",
                "inspect", "importlib", "pkgutil", "platform", "gc",
                "ast", "dis", "tokenize", "keyword",
                "pickle", "csv", "xml", "html", "gzip", "zipfile", "tarfile",
                "argparse", "configparser", "getopt",
                "unittest", "pytest",
                "IPython", "ipywidgets", "ipython",
            }:
                unknown_pkgs.add(imp)

    max_risk = "NONE"
    risk_order = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "NONE": 0}
    for rc in risky_cells:
        for f in rc["findings"]:
            if risk_order.get(f["risk"], 0) > risk_order.get(max_risk, 0):
                max_risk = f["risk"]

    return {
        "path": nb_path,
        "total_cells": len([c for c in cells if c.get("cell_type") == "code"]),
        "risky_cells": risky_cells,
        "max_risk": max_risk,
        "all_imports": sorted(all_imports),
        "unknown_packages": sorted(unknown_pkgs),
    }


def scan_manifest(manifest_path: str, import_index: dict[str, str]) -> list[dict]:
    """Scan all notebooks listed in a DAG manifest JSON."""
    with open(manifest_path) as f:
        manifest = json.load(f)

    notebook_paths = set()
    # Support both list-of-tasks and dict-of-tasks formats
    tasks = manifest if isinstance(manifest, list) else manifest.get("tasks", manifest.values() if isinstance(manifest, dict) else [])
    if isinstance(tasks, dict):
        tasks = tasks.values()
    for task in tasks:
        if isinstance(task, dict):
            path = task.get("notebook_path") or task.get("path")
            if path:
                notebook_paths.add(path)
            for dep in task.get("dependencies", []):
                if isinstance(dep, str):
                    notebook_paths.add(dep)
                elif isinstance(dep, dict):
                    dp = dep.get("notebook_path") or dep.get("path")
                    if dp:
                        notebook_paths.add(dp)

    results = []
    for nb_path in sorted(notebook_paths):
        if not nb_path.endswith(".ipynb") and not nb_path.endswith(".py"):
            nb_path = nb_path + ".ipynb"
        if os.path.exists(nb_path):
            results.append(scan_notebook(nb_path, import_index))
        else:
            results.append({"path": nb_path, "error": "file not found"})
    return results


def _risk_emoji(risk: str) -> str:
    return {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢", "NONE": "✅"}.get(risk, "⚪")


def generate_report(results: list[dict], unknown_packages: bool = False) -> str:
    lines = ["# AIDP FUSE Risk Scan Report", ""]

    high = [r for r in results if r.get("max_risk") == "HIGH"]
    medium = [r for r in results if r.get("max_risk") == "MEDIUM"]
    low = [r for r in results if r.get("max_risk") == "LOW"]
    errors = [r for r in results if "error" in r]
    clean = [r for r in results if r.get("max_risk") == "NONE"]

    lines.append(f"**Scanned:** {len(results)} notebooks | "
                 f"🔴 HIGH: {len(high)} | 🟡 MEDIUM: {len(medium)} | 🟢 LOW: {len(low)} | ✅ Clean: {len(clean)} | ⚠️ Error: {len(errors)}")
    lines.append("")

    if not high and not medium and not low:
        lines.append("No FUSE risks detected.")
        return "\n".join(lines)

    # Summary table
    lines.append("## Summary")
    lines.append("")
    lines.append("| Risk | Notebook | Risky Cells | Packages |")
    lines.append("|------|----------|-------------|---------|")
    for r in sorted(results, key=lambda x: {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "NONE": 3, None: 4}.get(x.get("max_risk"), 4)):
        if r.get("max_risk") in ("HIGH", "MEDIUM", "LOW"):
            pkgs = set()
            for rc in r.get("risky_cells", []):
                for f in rc["findings"]:
                    pkgs.add(f["package"])
            emoji = _risk_emoji(r["max_risk"])
            nb_name = os.path.basename(r["path"])
            lines.append(f"| {emoji} {r['max_risk']} | `{nb_name}` | {len(r['risky_cells'])} | {', '.join(sorted(pkgs))} |")
    lines.append("")

    # Detailed findings
    lines.append("## Detailed Findings")
    lines.append("")
    for r in results:
        if not r.get("risky_cells"):
            continue
        emoji = _risk_emoji(r.get("max_risk", "NONE"))
        lines.append(f"### {emoji} `{r['path']}`")
        lines.append("")
        for rc in r["risky_cells"]:
            lines.append(f"**Cell {rc['cell_idx']}:**")
            for f in rc["findings"]:
                lines.append(f"- **{f['package']}** ({f['risk']}): {f['reason']}")
                lines.append(f"  - *Fix:* {f['mitigation']}")
                if f.get("volumes_path_present"):
                    lines.append("  - ⚠️ `/Volumes/` path detected in this cell — FUSE exposure confirmed.")
        lines.append("")

    # Unknown packages section
    if unknown_packages:
        all_unknown: set[str] = set()
        for r in results:
            all_unknown.update(r.get("unknown_packages", []))
        if all_unknown:
            lines.append("## Unknown Packages (not in FUSE risk DB)")
            lines.append("")
            lines.append("Run `python3 $HOME/.aidp-migrator/engine/scripts/fuse_package_analyzer.py --packages` to analyze these:")
            lines.append("")
            lines.append("```")
            lines.append("python3 $HOME/.aidp-migrator/engine/scripts/fuse_package_analyzer.py --packages " + " ".join(sorted(all_unknown)))
            lines.append("```")
            lines.append("")
            lines.append("| Package | Notebooks |")
            lines.append("|---------|----------|")
            for pkg in sorted(all_unknown):
                nbs = [os.path.basename(r["path"]) for r in results if pkg in r.get("unknown_packages", [])]
                lines.append(f"| `{pkg}` | {len(nbs)} |")
            lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="AIDP FUSE Dependency Scanner")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest", help="Path to DAG manifest JSON")
    group.add_argument("--notebook", help="Path to single .ipynb file")
    parser.add_argument("--output", help="Write Markdown report to this path (default: stdout)")
    parser.add_argument("--unknown-packages", action="store_true",
                        help="List packages not in the FUSE risk DB (candidate for fuse_package_analyzer.py)")
    parser.add_argument("--db", default=os.path.join(os.path.dirname(__file__), "fuse_risk_db.json"),
                        help="Path to external fuse_risk_db.json (default: $HOME/.aidp-migrator/engine/scripts/fuse_risk_db.json)")
    args = parser.parse_args()

    _load_external_db(args.db)
    import_index = _build_import_index()

    if args.manifest:
        results = scan_manifest(args.manifest, import_index)
    else:
        results = [scan_notebook(args.notebook, import_index)]

    report = generate_report(results, unknown_packages=args.unknown_packages)

    if args.output:
        with open(args.output, "w") as f:
            f.write(report)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
