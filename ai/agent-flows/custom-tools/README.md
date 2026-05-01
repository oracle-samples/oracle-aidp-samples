# Custom Tools Samples

A collection of example **custom tools** for AIDP Agent Flows. Custom tools let you write Python code that an agent can call during conversations — package your code as a ZIP, upload it to a workspace volume, and wire it into an agent flow through the UI.

For the full authoring contract (lifecycle hooks, config/template substitution, auth, input schema, packaging rules, troubleshooting), read the **[Custom Tools User Guide](./USER_GUIDE.md)** first.

## Samples

| Sample | What it shows |
|---|---|
| [hello-tool](./hello-tool/) | The minimal `CustomToolBase` tool — one class, one parameter, no dependencies. Start here. |
| [developer-toolkit](./developer-toolkit/) | Three tools in one package — bash execution, file I/O, and Python subprocess execution. Demonstrates multi-tool packages, defensive config coercion, and a `utils/` module. |
| [ords-database-tool](./ords-database-tool/) | Query Oracle Autonomous Database over the ORDS REST API. Three tools: SQL execution, schema exploration, and schema listing. Demonstrates credentialed HTTP calls and text table formatting. |

## How to use a sample

1. Pick a sample folder and review its `README.md`.
2. Edit `tool_config.json` — fill in any placeholders (endpoints, credentials, OCIDs) for your environment.
3. Build the ZIP with the build utility:
   ```bash
   python scripts/build_custom_tool.py hello-tool
   ```
   The script validates the structure, auto-generates any missing `__init__.py` markers, cross-checks `tool_config.json` against the classes you registered, warns about any `requirements.txt` entries the platform will drop, enforces the platform's size limits, and writes `custom_tool_<source_key>.zip`. See [`scripts/README.md`](./scripts/README.md) for flags and exit codes. A plain `zip -r` also works if you prefer.
4. Upload the ZIP to your AIDP workspace volume through the file browser.
5. In the agent flow editor, add a **Custom Tool** node and paste the values from `tool_config.json` into the configuration panel. The package path is the volume path of the ZIP you uploaded.
6. Attach a compute resource, deploy, and test in the Playground.

## Building your own tool

Once you're ready to write your own, the fastest path is:

1. Copy `hello-tool/` to a new folder (for example, `my-tool/`).
2. Rewrite `tool_implementation.py` — keep one or more `@CustomToolBase.register` classes and override `_execute_tool`.
3. Update `tool_config.json` with your class name(s), input schema, and default config values.
4. Add any dependencies to `requirements.txt` (the build utility will warn about anything the platform would drop).
5. Run `python scripts/build_custom_tool.py my-tool --validate` to confirm the structure is clean, then drop `--validate` to produce the ZIP.

### Offline install (bundled wheels)

If you want the tool to install without touching the network on compute — for production tools, restricted environments, or faster test-tool runs — add `--bundle-wheels`:

```bash
python scripts/build_custom_tool.py my-tool --bundle-wheels
```

The utility runs `pip download` with the compute runtime's target (Linux amd64 / Python 3.11), stages the wheels under `my-tool/wheels/`, and packages them into the final ZIP. The platform detects the `wheels/` directory and installs from it first via `pip install --no-index --find-links ./wheels/`. See [scripts/README.md](./scripts/README.md#bundling-wheels) for the strategy and size-limit notes.

## File conventions

Every sample follows the same on-disk layout:

```
<sample-name>/
├── README.md                 # walkthrough + configuration reference
├── tool_implementation.py    # required — contains @CustomToolBase.register classes
├── tool_config.json          # reference values to paste into the UI
├── requirements.txt          # pip dependencies (filtered by the platform)
├── utils/                    # optional — shared helper modules (with __init__.py)
├── config/                   # optional — static config files the code reads at runtime
└── data/                     # optional — static data files
```

Each sample's `tool_config.json` is a **human-readable reference**, not a file the platform consumes directly. Copy its values into the matching fields in the agent flow UI.

## See also

- [USER_GUIDE.md](./USER_GUIDE.md) — full reference: lifecycle hooks, MCP output, input schema, auth, wheel bundling, common pitfalls, troubleshooting.
- [../README.md](../README.md) — Agent Flows samples overview (visual-flow and code-first).
