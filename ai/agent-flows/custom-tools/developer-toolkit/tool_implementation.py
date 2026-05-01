import subprocess
import os

from aidputils.agents.tools.custom_tools.base import CustomToolBase
from .utils.text_utils import truncate_output, sanitize_path


def _get_cfg(conf, key, default):
    """Read a config value from either the outer AIDPToolConf dict
    (``conf[key]``) or the nested user conf (``conf["conf"][key]``).
    Coerces numeric settings to int to avoid type mismatches when the
    value is rendered as a string by the template/codegen layer."""
    inner = conf.get("conf") if isinstance(conf, dict) else None
    if isinstance(inner, dict) and key in inner:
        value = inner[key]
    elif isinstance(conf, dict) and key in conf:
        value = conf[key]
    else:
        value = default
    if isinstance(default, int) and not isinstance(value, bool):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    return value


@CustomToolBase.register
class BashTool(CustomToolBase):
    """Execute bash commands and return output."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        command = runtime_params.get("command", "")
        timeout = _get_cfg(conf, "timeout", 30)
        max_lines = _get_cfg(conf, "max_output_lines", 200)
        try:
            result = subprocess.run(
                ["bash", "-c", command],
                capture_output=True, text=True, timeout=timeout
            )
            output = result.stdout or ""
            if result.stderr:
                output += "\n[stderr]\n" + result.stderr
            return {"output": truncate_output(output, max_lines)}
        except subprocess.TimeoutExpired:
            return {"error": f"Command timed out after {timeout}s"}
        except Exception as e:
            return {"error": str(e)}


@CustomToolBase.register
class FileTool(CustomToolBase):
    """Read, write, or list files in the workspace."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        operation = runtime_params.get("operation", "")
        path = runtime_params.get("path", "")
        content = runtime_params.get("content", "")
        base_dir = _get_cfg(conf, "base_dir", "/workspace")
        max_size = _get_cfg(conf, "max_file_size_kb", 1024) * 1024

        safe_path = sanitize_path(base_dir, path)
        if safe_path is None:
            return {"error": "Invalid path: path traversal detected"}

        try:
            if operation == "read":
                with open(safe_path, "r") as f:
                    return {"output": f.read()}
            elif operation == "write":
                parent = os.path.dirname(safe_path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(safe_path, "w") as f:
                    f.write(content)
                return {"output": f"Written {len(content)} chars to {path}"}
            elif operation == "list":
                target = safe_path if os.path.isdir(safe_path) else os.path.dirname(safe_path)
                return {"output": "\n".join(sorted(os.listdir(target)))}
            else:
                return {"error": f"Unknown operation: {operation}. Use read/write/list"}
        except Exception as e:
            return {"error": str(e)}


@CustomToolBase.register
class PythonTool(CustomToolBase):
    """Execute Python code in an isolated subprocess."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        code = runtime_params.get("code", "")
        timeout = _get_cfg(conf, "timeout", 60)
        max_lines = _get_cfg(conf, "max_output_lines", 500)
        try:
            result = subprocess.run(
                ["python3", "-c", code],
                capture_output=True, text=True, timeout=timeout
            )
            output = result.stdout or ""
            if result.stderr:
                output += "\n[stderr]\n" + result.stderr
            return {"output": truncate_output(output, max_lines)}
        except subprocess.TimeoutExpired:
            return {"error": f"Execution timed out after {timeout}s"}
        except Exception as e:
            return {"error": str(e)}
