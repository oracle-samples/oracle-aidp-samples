#!/usr/bin/env python3
"""
build_custom_tool.py — Package an AIDP custom tool source directory into
a ZIP ready to upload to a workspace volume.

The script walks the same checks the platform performs at upload time, so
mistakes are caught locally before a failed deploy:

  1. `tool_implementation.py` must exist and register at least one class
     via @CustomToolBase.register (or @BaseTool.register).
  2. `tool_config.json` (if present) must reference class names that exist
     in the implementation file.
  3. `requirements.txt` (if present) is scanned for:
       - packages that will be dropped by the platform filter (conflict
         with runtime)
       - packages already pre-installed on compute (redundant)
       - URL / VCS / editable installs (blocked)
  4. Missing `__init__.py` markers are created under every subdirectory
     that holds Python files, so relative imports keep working once the
     ZIP is extracted under custom_tools/<source_key>/ on compute.
  5. Per-file (10 MB) and total extracted (500 MB) size limits are enforced.
  6. The final archive is named `custom_tool_<source_key>.zip` (matching
     the platform's source_key sanitization) and contains the directory
     contents — not the directory itself.

Usage
-----

    python build_custom_tool.py <source_dir>
    python build_custom_tool.py <source_dir> -o dist/hello.zip
    python build_custom_tool.py <source_dir> --validate

Exit codes
----------
    0  ZIP built, or --validate passed
    1  validation failed; ZIP not written
    2  bad CLI usage
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


# ---------------------------------------------------------------------------
# Compute runtime target (wheel downloads need to match this)
#
# The AIDP compute image is based on Oracle Linux 8 with Python 3.11
# (base image: `ol8-graalvm21-ee-17-jdk-python311`). OL 8 ships glibc 2.28,
# which under PEP 600 maps to the `manylinux_2_28_x86_64` tag.
#
# pip resolves `--platform manylinux_2_28_x86_64` as a *superset* of older
# manylinux tags — it accepts wheels tagged `manylinux_2_17`..`manylinux_2_28`,
# the legacy `manylinux2014_x86_64` / `manylinux2010_x86_64` / `manylinux1_x86_64`
# aliases, and `linux_x86_64`. So this default yields the widest set of
# installable wheels while still guaranteeing every wheel runs on the
# OL 8 compute. Packages that only ship newer `manylinux_2_34`+ wheels are
# correctly rejected — they'd crash at import time on OL 8.
# ---------------------------------------------------------------------------

DEFAULT_WHEEL_PLATFORM = "manylinux_2_28_x86_64"
DEFAULT_PYTHON_VERSION = "3.11"
WHEELS_SUBDIR = "wheels"


# ---------------------------------------------------------------------------
# Platform constraints
#
# The source of truth for these lists is the Custom Tools User Guide
# (../USER_GUIDE.md). Keep them in sync.
# ---------------------------------------------------------------------------

MAX_FILE_BYTES = 10 * 1024 * 1024           # 10 MB per file
MAX_TOTAL_BYTES = 500 * 1024 * 1024         # 500 MB total extracted

# Packages the platform drops from requirements.txt — they would replace
# pinned runtime components and break the agent. Mirrors
# `PackageManager._BASE_PACKAGE_NAMES` in
# datahub-dp/aidp-dp-agent/agentservice/utils/package_manager.py.
BLOCKED_PACKAGES = {
    "langgraph",
    "langchain-oci",
    "langchain-core",
    "langchain_mcp_adapters",
    "pyyaml",
}

# Packages already installed on the compute runtime. Listing them is
# redundant (the platform skips them) but harmless. Mirrors
# `PackageManager._PREINSTALLED_PACKAGE_NAMES`.
PREINSTALLED_PACKAGES = {
    "oci", "requests", "requests-toolbelt", "websockets",
    "cryptography", "certifi", "pyopenssl", "urllib3",
    "pydantic", "pydantic-core", "pydantic-settings",
    "numpy", "oracledb", "sqlalchemy",
    "aiohttp", "httpx", "httpx-sse", "anyio",
    "jsonschema", "orjson",
}

REGISTRATION_MARKERS = (
    "CustomToolBase.register",
    "BaseTool.register",
)

# Housekeeping files that should never be packaged.
EXCLUDE_PATTERNS = [
    re.compile(r"(^|/)\.git(/|$)"),
    re.compile(r"(^|/)\.DS_Store$"),
    re.compile(r"(^|/)__pycache__(/|$)"),
    re.compile(r".*\.pyc$"),
    re.compile(r".*\.pyo$"),
    re.compile(r"(^|/)\.pytest_cache(/|$)"),
    re.compile(r"(^|/)\.mypy_cache(/|$)"),
    re.compile(r".*\.egg-info(/|$)"),
    re.compile(r"(^|/)\.ipynb_checkpoints(/|$)"),
]


# ---------------------------------------------------------------------------
# Pretty output
# ---------------------------------------------------------------------------

def _info(msg: str) -> None:
    print(f"  {msg}")


def _ok(msg: str) -> None:
    print(f"  ok   {msg}")


def _warn(msg: str) -> None:
    print(f"  warn {msg}")


def _err(msg: str) -> None:
    print(f"  err  {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_excluded(rel_path: str) -> bool:
    return any(p.search(rel_path) for p in EXCLUDE_PATTERNS)


_MAX_SOURCE_KEY_LENGTH = 128


def sanitize_source_key(name: str) -> str:
    """Mirror the platform's source_key derivation byte-for-byte.

    Source of truth: `extract_source_key` in
    datahub-dp/aidp-dp-agent/agentservice/utils/aidp_utils.py (which in
    turn matches Java's `CodeGenContext.extractSourceKeyFromPackagePath`).
    Drift here causes the locally built ZIP filename to disagree with the
    `custom_tools/<source_key>/` directory the platform's codegen imports
    from — the deploy then fails at import time with no obvious clue.

    Steps:
      1. Reject path traversal (`..` in any path component → "unknown").
      2. Strip leading `/`, take the last path segment.
      3. Drop a trailing `.zip`.
      4. Replace `-` with `_`.
      5. Replace any non-`\\w` char with `_` (re.ASCII so the regex
         matches Java's `\\w`, otherwise Unicode letters like accented
         chars or CJK pass through and produce a different key than
         Java).
      6. Prepend `_` if the first char isn't `[a-zA-Z_]`.
      7. Truncate to MAX length.
    """
    if not name:
        return "unknown"

    import os
    normed = os.path.normpath(name)
    parts = normed.replace("\\", "/").split("/")
    if ".." in parts:
        return "unknown"
    original_depth = len([p for p in name.replace("\\", "/").split("/") if p and p != "."])
    normed_depth = len([p for p in parts if p and p != "."])
    if normed_depth < original_depth:
        return "unknown"

    stem = name.lstrip("/").rsplit("/", 1)[-1]
    if stem.endswith(".zip"):
        stem = stem[:-4]
    stem = stem.replace("-", "_")
    stem = re.sub(r"[^\w]", "_", stem, flags=re.ASCII)
    if stem and not (stem[0].isalpha() or stem[0] == "_"):
        stem = "_" + stem
    stem = stem[:_MAX_SOURCE_KEY_LENGTH]
    return stem or "unknown"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def find_registered_classes(impl_path: Path) -> list[str]:
    """Return class names decorated with a recognized registration marker."""
    try:
        tree = ast.parse(impl_path.read_text())
    except SyntaxError as exc:
        raise ValueError(f"tool_implementation.py has a syntax error: {exc}") from None

    classes: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for dec in node.decorator_list:
            src = _unparse_decorator(dec)
            if any(marker in src for marker in REGISTRATION_MARKERS):
                classes.append(node.name)
                break
    return classes


def _unparse_decorator(node: ast.AST) -> str:
    """Minimal unparser so Python 3.8 works too (ast.unparse lands in 3.9)."""
    unparse = getattr(ast, "unparse", None)
    if unparse is not None:
        return unparse(node)
    if isinstance(node, ast.Call):
        return _unparse_decorator(node.func)
    if isinstance(node, ast.Attribute):
        return f"{_unparse_decorator(node.value)}.{node.attr}"
    if isinstance(node, ast.Name):
        return node.id
    return ""


def validate_tool_config(config_path: Path, registered: list[str]) -> list[str]:
    """Return warnings for mismatches between tool_config.json and the code."""
    try:
        data = json.loads(config_path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"tool_config.json is not valid JSON: {exc}") from None

    warnings: list[str] = []
    tools = data.get("tools", [])
    if not isinstance(tools, list) or not tools:
        warnings.append("tool_config.json has no non-empty `tools` array.")
        return warnings

    registered_set = set(registered)
    for i, entry in enumerate(tools):
        cls = entry.get("toolClassName") or entry.get("className")
        if not cls:
            warnings.append(f"tool_config.json[{i}] is missing toolClassName.")
            continue
        if cls not in registered_set:
            warnings.append(
                f"tool_config.json references `{cls}`, "
                f"which is not registered in tool_implementation.py."
            )
    return warnings


_REQ_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")

_URL_VCS_PREFIXES = (
    "-e ", "--editable",
    "git+", "hg+", "svn+", "bzr+",
    "http://", "https://", "file:",
)


def _is_url_vcs_requirement(line: str) -> bool:
    """Match the platform's URL/VCS detection in
    `PackageManager._merge_custom_tool_requirements`. Catches both
    prefix forms (`git+`, `https://`, …) and PEP 508 direct-URL refs
    (`pkg @ https://…`, `pkg @ file:…`)."""
    if any(line.startswith(p) for p in _URL_VCS_PREFIXES):
        return True
    return "@" in line and ("://" in line or "file:" in line)


def _canonicalize_name(name: str) -> str:
    """Match the platform's `lower().replace('-', '_')` membership check.

    Mirrors `_merge_custom_tool_requirements` in
    datahub-dp/aidp-dp-agent/agentservice/utils/package_manager.py so the
    build script's filtering predicts the platform's behavior exactly —
    `langchain_core` and `langchain-core` both normalize to
    `langchain_core` and match the entry in BLOCKED_PACKAGES.
    """
    return name.lower().replace("-", "_")


BLOCKED_PACKAGES_CANONICAL = {_canonicalize_name(p) for p in BLOCKED_PACKAGES}
PREINSTALLED_PACKAGES_CANONICAL = {_canonicalize_name(p) for p in PREINSTALLED_PACKAGES}


def check_requirements(req_path: Path) -> tuple[list[str], list[str], list[str]]:
    """Return (blocked, redundant, url_vcs) lines from requirements.txt."""
    blocked: list[str] = []
    redundant: list[str] = []
    url_vcs: list[str] = []

    for raw in req_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if _is_url_vcs_requirement(line):
            url_vcs.append(line)
            continue
        match = _REQ_NAME_RE.match(line)
        if not match:
            continue
        base = _canonicalize_name(match.group(1).split("[", 1)[0])
        if base in BLOCKED_PACKAGES_CANONICAL:
            blocked.append(line)
        elif base in PREINSTALLED_PACKAGES_CANONICAL:
            redundant.append(line)

    return blocked, redundant, url_vcs


def _installable_requirements(req_path: Path) -> list[str]:
    """Lines from requirements.txt that the platform would actually install.

    Filters out blocked packages (runtime conflict), pre-installed packages
    (redundant — already on compute), and URL / VCS / editable installs
    (blocked by the platform). The remainder is what needs wheels.
    """
    keep: list[str] = []
    for raw in req_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if _is_url_vcs_requirement(line):
            continue
        match = _REQ_NAME_RE.match(line)
        if not match:
            continue
        base = _canonicalize_name(match.group(1).split("[", 1)[0])
        if base in BLOCKED_PACKAGES_CANONICAL or base in PREINSTALLED_PACKAGES_CANONICAL:
            continue
        keep.append(line)
    return keep


def download_wheels(
    req_path: Path,
    wheels_dir: Path,
    python_version: str = DEFAULT_PYTHON_VERSION,
    platform: str = DEFAULT_WHEEL_PLATFORM,
) -> tuple[int, list[str]]:
    """Download wheels for every installable requirement into `wheels_dir`.

    Tries a single platform-targeted `pip download -r` first so pip can
    resolve transitive deps against the compute runtime's Linux amd64 /
    Python 3.11 target. If that fails (typically because one transitive
    dep doesn't ship a manylinux wheel and tanks the all-or-nothing
    resolution), retries each requirement individually with `--no-deps`
    while keeping the same `--platform` / `--python-version` /
    `--only-binary=:all:` constraints — without `--only-binary` pip would
    fall back to source distributions, which the platform can't build on
    the OL 8 compute (no C toolchain).

    Returns (wheel_count_after, failures) where `failures` is a list of
    requirement lines whose wheel could not be fetched.
    """
    requirements = _installable_requirements(req_path)
    if not requirements:
        return 0, []

    wheels_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix="-requirements.txt", delete=False
    ) as tmp:
        tmp.write("\n".join(requirements) + "\n")
        tmp_req = Path(tmp.name)

    failures: list[str] = []
    try:
        # First pass: resolve the whole set against the compute runtime.
        cmd = [
            sys.executable, "-m", "pip", "download",
            "--dest", str(wheels_dir),
            "--platform", platform,
            "--python-version", python_version,
            "--only-binary=:all:",
            "--no-cache-dir",
            "-r", str(tmp_req),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            stderr_tail = (result.stderr or result.stdout or "").strip().splitlines()
            for line in stderr_tail[-6:]:
                _info(line)
            _info("Retrying per requirement with --no-deps...")
            for req in requirements:
                retry = subprocess.run(
                    [
                        sys.executable, "-m", "pip", "download",
                        "--dest", str(wheels_dir),
                        "--platform", platform,
                        "--python-version", python_version,
                        "--only-binary=:all:",
                        "--no-deps",
                        "--no-cache-dir",
                        req,
                    ],
                    capture_output=True, text=True,
                )
                if retry.returncode != 0:
                    failures.append(req)
    finally:
        tmp_req.unlink(missing_ok=True)

    wheel_count = len(list(wheels_dir.glob("*.whl")))
    return wheel_count, failures


# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------

def ensure_init_files(root: Path) -> list[Path]:
    """Create empty __init__.py in every subdirectory that holds .py files."""
    created: list[Path] = []
    for directory in sorted(p for p in root.rglob("*") if p.is_dir()):
        rel = directory.relative_to(root).as_posix() + "/"
        if is_excluded(rel):
            continue
        has_py = any(
            child.is_file() and child.suffix == ".py"
            for child in directory.iterdir()
        )
        init_path = directory / "__init__.py"
        if has_py and not init_path.exists():
            init_path.write_text("")
            created.append(init_path)
    return created


def collect_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if is_excluded(rel):
            continue
        files.append(path)
    return files


def enforce_size_limits(files: list[Path]) -> int:
    total = 0
    for path in files:
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            raise ValueError(
                f"{path} is {size:,} bytes — "
                f"exceeds the {MAX_FILE_BYTES // (1024 * 1024)} MB per-file limit."
            )
        total += size
    if total > MAX_TOTAL_BYTES:
        raise ValueError(
            f"Total uncompressed size is {total:,} bytes — "
            f"exceeds the {MAX_TOTAL_BYTES // (1024 * 1024)} MB limit."
        )
    return total


def build_zip(root: Path, files: list[Path], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(files):
            arcname = path.relative_to(root).as_posix()
            zf.write(path, arcname=arcname)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="build_custom_tool",
        description=(
            "Package an AIDP custom tool source directory into an uploadable "
            "ZIP. Runs the same structural checks the platform performs."
        ),
    )
    parser.add_argument("source", help="Path to the custom tool source directory.")
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output ZIP path. Default: ./custom_tool_<source_key>.zip",
    )
    parser.add_argument(
        "-n", "--name",
        default=None,
        help="Override the source_key used in the default output filename.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run validation only; do not write a ZIP.",
    )
    parser.add_argument(
        "--no-init-gen",
        action="store_true",
        help="Do not auto-generate missing __init__.py files.",
    )
    parser.add_argument(
        "--bundle-wheels",
        action="store_true",
        help=(
            "Download wheels for every installable requirement into "
            "<source>/wheels/ so the tool installs offline on compute. "
            "Targets Linux amd64 / Python 3.11 by default."
        ),
    )
    parser.add_argument(
        "--wheel-platform",
        default=DEFAULT_WHEEL_PLATFORM,
        help=f"pip --platform value for wheel downloads (default: {DEFAULT_WHEEL_PLATFORM}).",
    )
    parser.add_argument(
        "--wheel-python-version",
        default=DEFAULT_PYTHON_VERSION,
        help=f"pip --python-version value for wheel downloads (default: {DEFAULT_PYTHON_VERSION}).",
    )
    args = parser.parse_args(argv)

    root = Path(args.source).resolve()
    if not root.is_dir():
        _err(f"Source directory does not exist: {root}")
        return 2

    print(f"Source: {root}")

    # tool_implementation.py is the one mandatory file.
    impl = root / "tool_implementation.py"
    if not impl.is_file():
        _err("tool_implementation.py is required but was not found.")
        return 1
    try:
        registered = find_registered_classes(impl)
    except ValueError as exc:
        _err(str(exc))
        return 1
    if not registered:
        _err(
            "tool_implementation.py contains no @CustomToolBase.register "
            "(or @BaseTool.register) class."
        )
        return 1
    _ok(f"Registered {len(registered)} class(es): {', '.join(registered)}")

    # tool_config.json is optional but checked when present.
    cfg = root / "tool_config.json"
    if cfg.is_file():
        try:
            warnings = validate_tool_config(cfg, registered)
        except ValueError as exc:
            _err(str(exc))
            return 1
        if warnings:
            for w in warnings:
                _warn(w)
        else:
            _ok("tool_config.json classes match the implementation.")
    else:
        _info("No tool_config.json — skipping config cross-check.")

    # requirements.txt: warnings only, never fatal.
    req = root / "requirements.txt"
    if req.is_file():
        blocked, redundant, url_vcs = check_requirements(req)
        for line in blocked:
            _warn(f"Platform will drop this package (runtime conflict): {line}")
        for line in redundant:
            _warn(f"Already pre-installed on compute (redundant): {line}")
        for line in url_vcs:
            _warn(f"URL / VCS install blocked by the platform: {line}")
        if not (blocked or redundant or url_vcs):
            _ok("requirements.txt looks clean.")
    else:
        _info("No requirements.txt — no dependencies to check.")

    # Optional: bundle wheels for an offline-install ZIP.
    if args.bundle_wheels:
        if not req.is_file():
            _info("--bundle-wheels requested but no requirements.txt — nothing to download.")
        else:
            wheels_dir = root / WHEELS_SUBDIR
            _info(
                f"Downloading wheels into {wheels_dir.relative_to(root)}/ "
                f"(target {args.wheel_platform} / Python {args.wheel_python_version})..."
            )
            count, failures = download_wheels(
                req, wheels_dir,
                python_version=args.wheel_python_version,
                platform=args.wheel_platform,
            )
            if count == 0 and not failures:
                _info(
                    "No wheels to download — every requirement is either "
                    "pre-installed on compute, blocked, or a URL/VCS install."
                )
            else:
                _ok(f"{count} wheel file(s) staged in {wheels_dir.relative_to(root)}/")
            for req_line in failures:
                _warn(
                    f"Could not download a wheel for `{req_line}`. "
                    f"Download it manually with `pip download --dest wheels/ "
                    f"--platform {args.wheel_platform} "
                    f"--python-version {args.wheel_python_version} "
                    "--only-binary=:all: ...` and re-run the build."
                )

    # Add __init__.py where relative imports need them.
    if not args.no_init_gen:
        created = ensure_init_files(root)
        if created:
            for init in created:
                _ok(f"Created {init.relative_to(root)}")
        else:
            _ok("All Python subdirectories already have __init__.py.")

    # Size checks.
    files = collect_files(root)
    try:
        total = enforce_size_limits(files)
    except ValueError as exc:
        _err(str(exc))
        return 1
    _ok(f"Packaging {len(files)} file(s), {total:,} bytes uncompressed.")

    if args.validate:
        print("\nValidation OK. No ZIP written (--validate was set).")
        return 0

    source_key = sanitize_source_key(args.name or root.name)
    output_path = (
        Path(args.output) if args.output
        else Path.cwd() / f"custom_tool_{source_key}.zip"
    )
    build_zip(root, files, output_path)
    size = output_path.stat().st_size
    print(f"\nWrote {output_path} ({size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
