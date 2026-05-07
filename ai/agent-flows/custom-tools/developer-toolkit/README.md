# Developer Toolkit

Three tools in a single package that together give an agent the building blocks of a developer assistant: run shell commands, read/write/list files in a workspace directory, and execute arbitrary Python in an isolated subprocess.

## Tools

| Class | Purpose |
|---|---|
| `BashTool` | Executes a bash command; returns truncated stdout/stderr. Timeout and max-line settings come from the tool config. |
| `FileTool` | Reads, writes, or lists files under a configured `base_dir`. Uses `sanitize_path` to block path-traversal escapes. |
| `PythonTool` | Executes Python code in an isolated `python3 -c` subprocess; returns truncated stdout/stderr. |

## What the sample demonstrates

- **Multi-tool packages.** A single ZIP registers three `CustomToolBase` classes; each gets its own entry in `tool_config.json`.
- **Defensive config coercion.** The `_get_cfg` helper reads values from either the top-level `conf` dict or the nested `conf["conf"]` payload, and coerces numeric settings (`timeout`, `max_output_lines`) to `int` — template substitution can turn numbers into strings, and a later `len(lines) > max_lines` comparison would crash otherwise. See *"Coerce numeric settings defensively"* in the [user guide](../USER_GUIDE.md).
- **A `utils/` module.** Shared helpers live in `utils/text_utils.py` (`truncate_output`, `sanitize_path`) and are imported with relative syntax (`from .utils.text_utils import ...`). The `utils/__init__.py` marker is included so imports resolve under `custom_tools/<source_key>/` on the compute runtime.
- **Static data and config.** `config/settings.yaml` and `data/prompts.txt` ship inside the ZIP; your code can read them at runtime.

## Files

```
developer-toolkit/
├── tool_implementation.py      # BashTool, FileTool, PythonTool
├── tool_config.json            # UI configuration reference for all three tools
├── requirements.txt            # stdlib only
├── utils/
│   ├── __init__.py
│   ├── text_utils.py           # truncate_output, sanitize_path
│   └── model_utils.py          # format_result helper
├── config/
│   └── settings.yaml           # default timeout and max_output_lines
└── data/
    └── prompts.txt             # static example content
```

## Walkthrough

1. **Package the ZIP** with the build utility. It picks up every directory under the source folder, adds missing `__init__.py` markers, and writes `custom_tool_developer_toolkit.zip`:
   ```bash
   python ../scripts/build_custom_tool.py .
   ```
2. **Upload** `custom_tool_developer_toolkit.zip` to your workspace volume.
3. **Add a Custom Tool node** for each of the three classes in your agent flow. Each entry in `tool_config.json` corresponds to one node (or one tool inside a multi-tool package node, depending on your UI).
4. **Review the defaults** in each tool's `conf` and override as needed:
   - `BashTool`: `timeout` (30s), `max_output_lines` (200).
   - `FileTool`: `base_dir` (`/workspace`), `max_file_size_kb` (1024).
   - `PythonTool`: `timeout` (60s), `max_output_lines` (500).
5. **Test** each tool from the UI:
   - Bash: `{"command": "ls /tmp"}`.
   - File: `{"operation": "list", "path": "."}`.
   - Python: `{"code": "print(2 ** 10)"}`.
6. **Deploy** to let the agent orchestrate the three tools.

## Security notes

- `BashTool` and `PythonTool` execute whatever code the LLM emits — treat them like a REPL. Only enable them in trusted flows or behind human-in-the-loop approval.
- `FileTool` sanitizes paths against `base_dir` using `os.path.normpath`; do not disable this check.
- Both execution tools enforce a timeout and truncate output. Increase the limits thoughtfully.

## See also

- [hello-tool](../hello-tool/) — the minimal starting template.
