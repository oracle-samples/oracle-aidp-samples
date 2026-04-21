import json

def format_result(tool_name, output, error=False):
    return json.dumps({"tool": tool_name, "status": "error" if error else "success", "output": output}, indent=2)
