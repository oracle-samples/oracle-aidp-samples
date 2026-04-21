# Custom Tools for AIDP Agent Flows

Custom tools let you write Python code that your agent can call as tools during conversations. Package your code as a ZIP, upload it to your workspace, and configure it in your agent flow.

## Quick Start

### 1. Write your tool

Create a Python file that extends `CustomToolBase`:

```python
# tool_implementation.py
from aidputils.agents.tools.custom_tools.base import CustomToolBase

@CustomToolBase.register
class HelloTool(CustomToolBase):
    """A simple greeting tool."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        name = runtime_params.get("name", "World")
        return {"greeting": f"Hello, {name}!"}
```

### 2. Package as ZIP

```
my_tool.zip
├── tool_implementation.py    # Required: your tool class(es)
├── requirements.txt          # Optional: pip dependencies
├── utils/                    # Optional: helper modules
│   ├── __init__.py
│   └── helpers.py
└── config/                   # Optional: config files
    └── settings.yaml
```

Create the ZIP:
```bash
cd my_tool/
zip -r my_tool.zip tool_implementation.py requirements.txt utils/ config/
```

### 3. Upload to workspace

Upload `my_tool.zip` to your workspace volume through the UI file browser.

### 4. Add to agent flow

In the agent flow editor, add a Custom Tool node and configure:
- **Package Path**: the workspace path to your ZIP (e.g., `Agent_flows/my_project/my_tool.zip`)
- **Tool Class Name**: the Python class name (e.g., `HelloTool`)
- **Display Name**: human-readable name shown to the LLM
- **Description**: what the tool does (helps the LLM decide when to use it)
- **Input Schema**: parameters the LLM can pass to the tool

### 5. Test

Use the Test Tool button in the UI, or call the API:
```bash
POST /tools/actions/test
{
  "toolType": "CUSTOM",
  "toolConfig": {
    "packagePath": "Agent_flows/my_project/my_tool.zip",
    "tools": [{
      "toolClassName": "HelloTool",
      "displayName": "Hello Tool",
      "description": "Returns a greeting",
      "inputSchema": {
        "name": {"name": "name", "type": "string", "description": "Name to greet"}
      }
    }]
  },
  "paramValues": {"values": {"name": "Alice"}}
}
```

### 6. Deploy

Attach a compute resource and deploy the agent flow. The deployment installs your requirements and starts the agent.

---

## Tool Implementation

### The `_execute_tool` method

This is the only method you must implement. It receives:

| Parameter | Type | Description |
|-----------|------|-------------|
| `conf` | `dict` | Tool configuration from `config` in the YAML spec. Access nested config via `conf.get("conf", conf)` |
| `runtime_params` | `dict` | Input parameters passed by the LLM at invocation time |
| `**context_vars` | `dict` | System context (e.g., `datalake_id`) |

Return a `dict`. The framework wraps it into MCP format automatically.

### Return values

```python
# Simple result
return {"answer": "42"}

# Error
return {"error": "Something went wrong"}

# Rich result
return {
    "summary": "Found 3 records",
    "records": [{"id": 1}, {"id": 2}, {"id": 3}],
    "metadata": {"query_time_ms": 45}
}
```

### Optional lifecycle hooks

```python
@CustomToolBase.register
class MyTool(CustomToolBase):

    @classmethod
    def _validate_config(cls, conf, runtime_params, **context_vars):
        """Called before execution. Raise ValueError to abort."""
        if not conf.get("conf", {}).get("api_key"):
            raise ValueError("api_key is required in tool config")

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        """Required. Your tool logic here."""
        return {"result": "ok"}

    @classmethod
    def _transform_response(cls, response):
        """Called after execution. Transform the response before MCP formatting."""
        if isinstance(response, dict) and "raw_data" in response:
            return {"summary": f"Processed {len(response['raw_data'])} items"}
        return response
```

### Making HTTP requests

Use the built-in `_make_http_request` helper which includes SSRF protection and auth support:

```python
@classmethod
def _execute_tool(cls, conf, runtime_params, **context_vars):
    response = cls._make_http_request(
        method="GET",
        url="https://api.example.com/data",
        conf=conf,
        headers={"Accept": "application/json"},
        timeout=30
    )
    return response.json()
```

### Multiple tools per package

A single ZIP can contain multiple tool classes:

```python
# tool_implementation.py

@CustomToolBase.register
class SearchTool(CustomToolBase):
    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        query = runtime_params.get("query", "")
        return {"results": [f"Result for: {query}"]}

@CustomToolBase.register
class SummarizeTool(CustomToolBase):
    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        text = runtime_params.get("text", "")
        return {"summary": text[:100] + "..."}
```

Configure each tool separately in the agent flow with its own `toolClassName`, `displayName`, `description`, and `inputSchema`.

---

## Dependencies (requirements.txt)

### Supported formats

```
# Simple version specs
humanize>=4.0
python-dateutil>=2.8,<3.0
beautifulsoup4==4.12.3

# Comments are fine
# This is a comment
```

### Filtered automatically

The platform filters your requirements to avoid conflicts:

| Category | Examples | Action |
|----------|----------|--------|
| Platform packages | `langgraph`, `langchain-core`, `pyyaml` | **Discarded** (would break the agent runtime) |
| Pre-installed | `requests`, `oci`, `numpy`, `oracledb`, `pydantic`, `aiohttp` | **Skipped** (already available) |
| URL/VCS installs | `git+https://...`, `-e ./local_pkg` | **Blocked** (security) |
| Everything else | `humanize`, `beautifulsoup4`, etc. | **Included** |

### When dependencies are installed

| Context | Pip Install | Network Required |
|---------|-------------|-----------------|
| **Test Tool** (one-shot) | Attempted before execution, fails gracefully if no network | Yes (Artifactory) |
| **Deployment** (long-running) | Installed during deployment startup into isolated venv | Yes (Artifactory) |

Each deployment gets its own isolated virtual environment. When you redeploy (add/remove tools), a fresh venv is created. No dependency leakage between deployments.

### Bundling wheel files (offline install)

If your tool needs dependencies that aren't pre-installed, you can bundle `.whl` files in your ZIP for reliable installation without network access. This is the recommended approach for production tools.

**Step 1: Download wheels**

```bash
# Download wheels for your requirements into a wheels/ directory.
# The compute runtime is Oracle Linux 8 (glibc 2.28) with Python 3.11,
# which under PEP 600 maps to manylinux_2_28_x86_64. pip treats this tag
# as a superset of older manylinux tags (manylinux2014, manylinux_2_17, ...)
# so you still get the widest possible pool of installable wheels.

pip download \
  --dest wheels/ \
  --platform manylinux_2_28_x86_64 \
  --python-version 3.11 \
  --only-binary=:all: \
  -r requirements.txt
```

If a package has no pre-built wheel for Linux (pure Python packages usually do), add `--no-binary :none:` or download without platform constraints:

```bash
# For pure-Python packages (no C extensions), platform doesn't matter
pip download --dest wheels/ --no-deps humanize>=4.0
```

**Step 2: Include in your ZIP**

```
my_tool.zip
├── tool_implementation.py
├── requirements.txt
└── wheels/
    ├── humanize-4.15.0-py3-none-any.whl
    └── python_dateutil-2.9.0-py2.py3-none-any.whl
```

```bash
cd my_tool/
zip -r my_tool.zip tool_implementation.py requirements.txt wheels/
```

**Step 3: That's it**

The platform detects the `wheels/` directory and installs from local files first:
- **Test execution**: `pip install --no-index --find-links ./wheels/ -r requirements.txt` (no network needed)
- **Deployment**: `pip install --find-links ./wheels/ -r requirements.txt` (local first, falls back to index)

**Quick recipe for common packages:**

```bash
mkdir wheels

# Pure Python packages (platform-independent)
pip download --dest wheels/ --no-deps humanize>=4.0
pip download --dest wheels/ --no-deps python-dateutil>=2.8
pip download --dest wheels/ --no-deps beautifulsoup4>=4.12

# Packages with C extensions (must target Linux x86_64, glibc 2.28 = OL 8)
pip download --dest wheels/ \
  --platform manylinux_2_28_x86_64 \
  --python-version 3.11 \
  --only-binary=:all: \
  numpy>=1.24

# Check what you have
ls wheels/
# humanize-4.15.0-py3-none-any.whl
# python_dateutil-2.9.0-py2.py3-none-any.whl
# ...
```

**Tip**: Wheels with `py3-none-any` in the filename are pure Python and work everywhere. Wheels with `manylinux` or `linux_x86_64` are platform-specific — make sure to download the Linux amd64 variant.

### Pre-installed packages (no requirements.txt needed)

These are available in the compute runtime without adding to `requirements.txt`:

```
requests, urllib3, certifi, aiohttp, httpx
oci, oracledb, sqlalchemy
numpy, pydantic, jsonschema
cryptography, pyopenssl
langchain-core, langchain-oci, langgraph
pyyaml, orjson, websockets
```

---

## Input Schema

Define what parameters the LLM can pass to your tool:

```json
{
  "inputSchema": {
    "query": {
      "name": "query",
      "type": "string",
      "description": "Search query to execute"
    },
    "limit": {
      "name": "limit",
      "type": "number",
      "description": "Maximum results to return"
    },
    "include_metadata": {
      "name": "include_metadata",
      "type": "boolean",
      "description": "Whether to include metadata in results"
    }
  }
}
```

Supported types: `string`, `number`, `boolean`.

The LLM reads the `description` to decide what values to pass. Write clear, specific descriptions.

---

## Tool Configuration

Static configuration values are passed via `config` in the tool spec. These are available in `conf` at runtime:

```yaml
tools:
  - toolClassName: OrdsSqlTool
    displayName: ORDS SQL Query
    config:
      ords_base_url: "https://my-adb.oraclecloudapps.com/ords/admin"
      username: "ADMIN"
      password: "{{db_password}}"    # Template variable
      timeout: 60
      max_rows: 100
```

### Template variables

Use `{{variable_name}}` in config values. These are substituted at runtime from the agent flow's parameter values:

```yaml
config:
  api_key: "{{my_api_key}}"
  base_url: "{{service_endpoint}}"
```

### Accessing config in code

The `conf` argument is the full `AIDPToolConf` dict (`name`, `description`, `tool_class`, `conf`, `params`, `auth`). **Your settings live under `conf["conf"]`**, not at the top level.

```python
@classmethod
def _execute_tool(cls, conf, runtime_params, **context_vars):
    tool_conf = conf.get("conf", conf)   # unwrap to user settings
    api_key = tool_conf.get("api_key", "")
    base_url = tool_conf.get("base_url", "https://default.example.com")
```

### Coerce numeric settings defensively

Config values flow through Jackson → Jinja → Python template substitution. Most of the time numbers stay as ints, but values that pass through `{{variable}}` substitution become strings. Any comparison like `len(lines) > max_lines` will then crash with `TypeError: '>' not supported between instances of 'int' and 'str'`.

Coerce once at read time:

```python
try:
    timeout = int(tool_conf.get("timeout", 30))
except (TypeError, ValueError):
    timeout = 30
```

The same applies to `float`, `bool` — don't trust the wire type when the value can come from a template.

### Return errors and set `isError`

When your tool fails, return an `error` key. The framework maps that to MCP with `isError: true` so the LLM sees the failure clearly. **Don't swallow exceptions and return a "success" shape that contains an error string** — the LLM has been observed to hallucinate rules like *"tool usage is currently forbidden per my guidelines"* after an ambiguous failure, poisoning the rest of the conversation.

```python
try:
    result = do_work()
    return {"output": result}
except Exception as e:
    return {"error": str(e)}        # → isError: true in MCP output
```

---

## Authentication

Custom tools support authentication for HTTP requests via the `auth` config:

```yaml
toolConfig:
  auth:
    authType: BEARER_TOKEN
    token: "{{session_token}}"
```

| Auth Type | Description |
|-----------|-------------|
| `BEARER_TOKEN` | Adds `Authorization: Bearer <token>` header |
| `OAUTH` | OAuth 2.0 client credentials flow |
| `OCI_RESOURCE_PRINCIPAL` | Uses OCI resource principal for OCI API calls |
| `NO_AUTH` | No authentication (default) |

Auth is automatically applied when using `_make_http_request()`.

---

## ZIP Package Structure

### Required

| File | Purpose |
|------|---------|
| `tool_implementation.py` | Must contain your `@CustomToolBase.register` decorated class(es) |

### Optional

| File/Directory | Purpose |
|----------------|---------|
| `requirements.txt` | Pip dependencies (filtered by platform) |
| `utils/`, `helpers/` | Python submodules (auto-generates `__init__.py` if missing) |
| `config/` | Configuration files your code can read at runtime |
| `data/` | Static data files |

### Constraints

| Limit | Value |
|-------|-------|
| Max file size | 10 MB per file |
| Max total extracted | 500 MB |
| Path traversal | Blocked (`../` not allowed) |
| Binary files | Supported (preserved as bytes) |

---

## Examples

### REST API Tool

```python
import json
import urllib.request
from aidputils.agents.tools.custom_tools.base import CustomToolBase

@CustomToolBase.register
class WeatherTool(CustomToolBase):
    """Get current weather for a city."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        city = runtime_params.get("city", "")
        if not city:
            return {"error": "city parameter is required"}

        tool_conf = conf.get("conf", conf)
        api_key = tool_conf.get("api_key", "")

        url = f"https://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
            return {
                "city": data["location"]["name"],
                "temperature_c": data["current"]["temp_c"],
                "condition": data["current"]["condition"]["text"]
            }
        except Exception as e:
            return {"error": str(e)}
```

### Database Query Tool (ORDS)

See `ords_tool.zip` for a complete example that:
- Connects to Oracle ADB via ORDS REST API
- Executes SQL queries with parameterized limits
- Explores schema (list tables, describe columns)
- Formats results as readable tables
- Includes two tools in one package (`OrdsSqlTool` + `OrdsSchemaExplorer`)

### Tool with Dependencies

```python
# tool_implementation.py
import humanize
from aidputils.agents.tools.custom_tools.base import CustomToolBase

@CustomToolBase.register
class FormatTool(CustomToolBase):
    """Format numbers in human-readable form."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        value = int(runtime_params.get("value", 0))
        return {
            "intword": humanize.intword(value),       # "1.5 billion"
            "naturalsize": humanize.naturalsize(value) # "1.4 GB"
        }
```

```
# requirements.txt
humanize>=4.0
```

---

## Common Pitfalls

### Model refuses to call tools after a failure

If a tool returns an error early in a chat, some models will produce a message like *"I'm sorry, but tool usage is currently forbidden per my guidelines"* and then stop calling tools for the rest of the session — even when the user explicitly asks. The hallucinated refusal lives in the conversation history and the model keeps obeying it.

Mitigations:
- Return structured errors (`{"error": "..."}`) so the framework marks `isError: true` and the model sees a real failure, not ambiguous output.
- Give the agent a real system prompt that tells it *when to use tools*. A placeholder prompt (`"aaaa"`) leaves the model with no guardrails and makes it more likely to invent its own.
- If the session is already poisoned, **start a new chat session** — clearing the thread is the fastest unstick.

### Placeholder tool descriptions

The `description` field on every tool entry is what the LLM reads to decide whether to call the tool. If it contains the UI's help text (*"The description field is meant to clearly explain what the tool does…"*) or is blank, the model has no idea what the tool does and will skip it. Write one or two sentences focused on **what the tool does** and **when to call it**.

### Config values arriving as strings

See *"Coerce numeric settings defensively"* above — always wrap `int(...)` / `float(...)` around config values you plan to compare or do arithmetic on.

### Relative imports

Files inside your ZIP are extracted under `custom_tools/<source_key>/`, so relative imports (`from .utils.text_utils import …`) work — **but only if every subdirectory has an `__init__.py`**. The platform auto-generates missing `__init__.py` files, but it's safer to ship them yourself.

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: No module named 'xxx'` | Missing dependency | Add to `requirements.txt`. If testing, dependency may not install (network restriction on test pods). Deploy the flow instead. |
| `ModuleNotFoundError: No module named 'langchain_core'` | Importing non-available package in test context | `langchain_core` is available on the compute runtime but not in all contexts. Deploy to test. |
| Tool returns `{"error": "..."}` | Your `_execute_tool` returned an error dict | Check the error message. This is your tool's own error handling working correctly. |
| `TypeError: '>' not supported between instances of 'int' and 'str'` | Numeric config value arrived as a string | Wrap reads with `int(...)` — see *"Coerce numeric settings defensively"*. |
| `ValueError: Invalid source key` | Package path has special characters | Use alphanumeric names with underscores. Avoid spaces and special characters in ZIP filename. |
| `isError: true` in response | Tool execution raised an exception | Check the `content` field for the traceback. Fix the bug in your tool code. |
| `isError: false` but the output contains an error message | Tool caught the exception and returned `{"output": "...error..."}` instead of `{"error": "..."}` | Use the `{"error": ...}` shape so the framework sets `isError` correctly. |
| Agent stops calling tools and says tools are "forbidden" | Session poisoned by an earlier tool error | See *"Model refuses to call tools after a failure"* — start a new session. |
| `Volume path does not exist` | ZIP not uploaded or wrong path | Upload via UI file browser. Use the path as shown in the file browser without the `/Workspace/` prefix. |
| `pip install failed` during test | Compute test pods have no network access | Dependencies install during full deployment, not test. For testing, use only pre-installed packages or deploy first. |
| `timeout` errors | Tool taking too long | Increase timeout in your tool config, or optimize the operation. Default compute timeout is 120 seconds. |
