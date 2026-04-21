from aidputils.agents.tools.custom_tools.base import CustomToolBase


@CustomToolBase.register
class HelloTool(CustomToolBase):
    """Return a greeting for a given name."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        name = runtime_params.get("name", "World")
        return {"greeting": f"Hello, {name}!"}
