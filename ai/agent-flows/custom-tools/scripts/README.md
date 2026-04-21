# `scripts/` — Custom Tool Build Utility

A small, stdlib-only Python script that packages a custom tool source directory into the ZIP the AIDP platform expects. It runs the same structural checks the platform performs at upload time, so problems surface locally instead of after a failed deploy.

## `build_custom_tool.py`

### What it does

1. **Requires `tool_implementation.py`** and parses it with `ast` to find classes decorated with `@CustomToolBase.register` (or `@BaseTool.register`).
2. **Cross-checks `tool_config.json`** (when present) — every `toolClassName` entry must resolve to a registered class.
3. **Scans `requirements.txt`** (when present) and warns about:
   - Packages the platform will drop because they conflict with the runtime (`langgraph`, `langchain-core`, `pyyaml`, …).
   - Packages already pre-installed on compute (`requests`, `oci`, `numpy`, …) — listing them is redundant.
   - URL / VCS / editable installs — blocked by the platform for security.
4. **Auto-generates `__init__.py`** under every subdirectory that contains `.py` files. Relative imports need these markers once the ZIP is extracted under `custom_tools/<source_key>/` on compute.
5. **Enforces the platform's size limits** — 10 MB per file, 500 MB total uncompressed.
6. **Writes the final archive** as `custom_tool_<source_key>.zip`, using the same `source_key` sanitization (`-` → `_`, strip non-word chars, leading-digit guard) the platform applies.
7. **Packages directory contents, not the directory itself** — which is what the platform expects.

### Usage

```bash
# From the scripts/ directory
python build_custom_tool.py ../hello-tool

# Or from anywhere in the repo
python ai/agent-flows/custom-tools/scripts/build_custom_tool.py \
    ai/agent-flows/custom-tools/hello-tool
```

Common flags:

| Flag | Purpose |
|---|---|
| `-o`, `--output <path>` | Write to a specific path instead of `./custom_tool_<name>.zip`. |
| `-n`, `--name <name>` | Override the name used in the default output filename. |
| `--validate` | Run every check but do not write a ZIP. Exit code `0` = clean, `1` = problems. |
| `--no-init-gen` | Skip auto-creation of `__init__.py` files. |
| `--bundle-wheels` | Download a wheel for every installable requirement into `<source>/wheels/` so the tool installs offline on compute (no network call during deploy / test). See "Bundling wheels" below. |
| `--wheel-platform <tag>` | Override pip's `--platform` for wheel downloads (default `manylinux_2_28_x86_64`). |
| `--wheel-python-version <ver>` | Override pip's `--python-version` for wheel downloads (default `3.11`). |

### Bundling wheels

`--bundle-wheels` runs `pip download` against your `requirements.txt` and stages the results under `<source>/wheels/`. The platform detects the `wheels/` directory and calls `pip install --no-index --find-links ./wheels/ -r requirements.txt` — no network round-trip at install time.

```
my-tool/
├── tool_implementation.py
├── requirements.txt              # humanize>=4.0
├── tool_config.json
└── wheels/                       # written by --bundle-wheels
    └── humanize-4.15.0-py3-none-any.whl
```

Strategy:

1. **Filter the requirements.** Blocked packages (`langgraph`, `pyyaml`, …), pre-installed packages (`requests`, `oci`, …), and URL/VCS installs are skipped — wheels for those would be wasted bytes or fail outright.
2. **First pass: the whole set, platform-targeted.** `pip download -r <filtered> --platform manylinux_2_28_x86_64 --python-version 3.11 --only-binary=:all:` runs once so pip can resolve transitive dependencies against the compute runtime.
3. **Retry pass: per-requirement `--no-deps`.** If the first pass fails (a pure-Python package without matching manylinux metadata is the usual culprit), each requirement is retried individually with `--no-deps`. Pure-Python wheels (`py3-none-any.whl`) download fine this way.
4. **Whatever still fails** is reported as a warning and the command exits with the code you'd normally get — the build continues so you can ship a partial wheels bundle and diagnose the rest manually.

The `wheels/` directory persists between builds, so re-runs are fast and cached. Commit it or add it to `.gitignore` as you prefer — it's just pre-downloaded binaries.

### Size limits

Wheels are subject to the same platform limits as the rest of the archive:

- **10 MB per file.** Libraries with large C extensions (e.g. `numpy`) will exceed this. If you need one, consider whether the package is already pre-installed on compute (check the list in the [user guide](../USER_GUIDE.md)) before bundling your own.
- **500 MB total uncompressed.** The build script enforces this before writing the ZIP.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | ZIP built (or `--validate` passed). |
| `1` | Validation failed — the ZIP was not written. |
| `2` | Bad CLI usage. |

### Example session

```
$ python build_custom_tool.py ../developer-toolkit
Source: /…/custom-tools/developer-toolkit
  ok   Registered 3 class(es): BashTool, FileTool, PythonTool
  ok   tool_config.json classes match the implementation.
  ok   requirements.txt looks clean.
  ok   All Python subdirectories already have __init__.py.
  ok   Packaging 10 file(s), 3,421 bytes uncompressed.

Wrote /…/custom_tool_developer_toolkit.zip (2,187 bytes)
```

### Requirements

- Python 3.8 or newer.
- No third-party packages — uses only the standard library (`ast`, `argparse`, `zipfile`, `pathlib`, `json`, `re`).

### Keeping in sync with the platform

The authoritative package lists live in the [Custom Tools User Guide](../USER_GUIDE.md) (*Dependencies → Filtered automatically* and *Pre-installed packages*). If the runtime's filter changes, update `BLOCKED_PACKAGES` / `PREINSTALLED_PACKAGES` at the top of `build_custom_tool.py` to match.

The `source_key` sanitizer mirrors `extract_source_key` in `aidp_utils.py` / `CodeGenContext.java`. Both the Python and Java sides must produce identical keys or imports will break on compute.
