# Hello Tool

The minimal custom tool — one class, one input parameter, no dependencies. Use this as a template when starting a new tool.

## What it does

`HelloTool` takes a `name` parameter and returns `{"greeting": "Hello, <name>!"}`. If no name is supplied it defaults to `"World"`.

## Files

| File | Purpose |
|---|---|
| `tool_implementation.py` | The tool class. |
| `tool_config.json` | Values to paste into the Custom Tool node in the agent flow UI. |
| `requirements.txt` | Empty — this sample uses only the standard library. |

## Walkthrough

1. **Package the ZIP** with the build utility — it validates the structure, adds any missing `__init__.py` markers, and writes `custom_tool_hello_tool.zip`:
   ```bash
   python ../scripts/build_custom_tool.py .
   ```
   (Or run a plain `zip -r ../hello_tool.zip tool_implementation.py requirements.txt` if you prefer. The script is a convenience, not a requirement.)
2. **Upload** `hello_tool.zip` to your AIDP workspace volume through the file browser. Note the path it lands at (for example `Agent_flows/demo/hello_tool.zip`).
3. **Create an agent flow** and drop a **Custom Tool** node into the canvas.
4. **Configure the node** using `tool_config.json`:
   - **Package path**: the volume path of the uploaded ZIP.
   - **Tool class name**: `HelloTool`.
   - **Display name** and **description**: copy from `tool_config.json`.
   - **Input schema**: one `string` parameter named `name`.
5. **Test the tool** from the UI's Test Tool button with `{"name": "Alice"}`. You should see `{"greeting": "Hello, Alice!"}`.
6. **Wire it into an agent**, attach a compute, and deploy. The agent can now call `HelloTool` during conversations.

## What to read next

- The [user guide](../USER_GUIDE.md) explains the full lifecycle (`_validate_config`, `_execute_tool`, `_transform_response`), MCP output, template substitution, and troubleshooting.
- [developer-toolkit](../developer-toolkit/) shows a multi-tool package with a shared `utils/` module.
