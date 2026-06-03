"""Accessing AIDP session variables in a high-code (type=CODE) agent flow.

Session variables are per-request, out-of-band context (user_id, tenant_id,
locale, ...) that the caller passes in the chat request's `metadata` field and
that your code can read — without the value appearing in the user's message.

This sample shows the three ways to access them:

  Pattern 1 — programmatically:  chat_context.session_context_var.get().get(...)
              (see get_session_var; used by the fetch_user_id tool and in invoke).
  Pattern 2 — via a tool template: a {{sessionvariables.<name>}} placeholder
              inside a built-in tool (the PromptTool reads topic_name).
  Pattern 3 — via create_get_dynamic_messages: render {{sessionvariables.<name>}}
              into the system prompt (tenant_id).

Worked example: pass user_id, tenant_id and topic_name as session variables; the
agent replies with the user_id (Pattern 1), 2 blog titles for topic_name
(Pattern 2), and the tenant_id (Pattern 3).
"""

from aidputils.agents import chat_context
from aidputils.agents.toolkit.agent_helper import (
    init_oci_llm,
    pre_invoke_setup,
    pre_tool_setup,
    post_tool_setup,
    create_get_dynamic_messages,
)
from aidputils.agents.toolkit.tool_helper import create_langgraph_tool
from aidputils.agents.toolkit.configs import OCIAIConf
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
import logging

logger = logging.getLogger("session-variables-example")
checkpointer = globals().get("checkpointer", None)

# ----- Replace with values for your compartment / region / OCI GenAI model -- #
compartment_ocid = "<your-compartment-ocid>"
region = "<oci-region>"
model_id = "<oci-genai-model-id>"          # e.g. an on-demand OCI GenAI model
endpoint = f"https://inference.generativeai.{region}.oci.oraclecloud.com"


# --- Pattern 1: read a session variable programmatically --------------------- #
def get_session_var(name: str, default=None):
    """Return a session variable's value by short name.

    The runtime stores each session variable as a {"name", "value"} dict under
    the lowercase key 'sessionvariables.<name>'. The isinstance(dict) check also
    guards the absent case (missing -> entry is None -> default).
    """
    entry = (chat_context.session_context_var.get() or {}).get(f"sessionvariables.{name}")
    return entry.get("value", default) if isinstance(entry, dict) else default


@tool("fetch_user_id", return_direct=False)
def fetch_user_id() -> str:
    """Return the caller's user_id from session variables (or 'unknown')."""
    return get_session_var("user_id") or "unknown"


# --- Pattern 2: read a session variable via a {{...}} template in a tool ------ #
# The PromptTool takes no runtime args; it reads the topic from the
# `sessionvariables.topic_name` session variable referenced in its template.
blog_titles_tool = create_langgraph_tool({
    "name": "generate_blog_titles",
    "description": "Generate 2 blog post titles. Takes no arguments; reads the "
                   "topic from the 'topic_name' session variable.",
    "tool_class": "PromptTool",
    "conf": {
        "llm": {
            "model_id": model_id,
            "model_provider": "generic",
            "model_args": {},
            "compartment_id": compartment_ocid,
            "endpoint": endpoint,
        },
        "prompt_template": (
            "Generate exactly TWO catchy, concise blog post titles about the topic "
            "'{{sessionvariables.topic_name}}'.\n"
            "Return ONLY a numbered list:\n1. <title one>\n2. <title two>"
        ),
    },
    "params": [],
})

agent_tools = [fetch_user_id, blog_titles_tool]

guardrails_config = {
    "name": "Default Guardrails",
    "description": "Default empty guardrails configuration",
    "policies": [],
}

llm_conf = OCIAIConf(
    model_provider="generic",
    compartment_id=compartment_ocid,
    model_args={},
    endpoint=endpoint,
    model_id=model_id,
    guardrails_config=guardrails_config,
)

# --- Pattern 3: render session variables into the system prompt -------------- #
# create_get_dynamic_messages returns a prompt callback that renders the template
# from the session context on every turn and returns the message list.
SYSTEM_TEMPLATE = (
    "You are a blog-title assistant for tenant '{{sessionvariables.tenant_id}}'.\n"
    "For any request, do all of the following, then answer in one reply:\n"
    "1. Call fetch_user_id to get the caller's user id.\n"
    "2. Call generate_blog_titles (no arguments) to get 2 blog titles.\n"
    "3. Reply with the user id, the tenant, and the 2 blog titles.\n"
    "- If a placeholder is left unresolved (literal {{...}}), that variable was "
    "not provided — respond without it."
)
dynamic_prompt_callback = create_get_dynamic_messages(SYSTEM_TEMPLATE)


class SessionVariableAgent:
    def __init__(self) -> None:
        self.agent = None

    def setup(self) -> None:
        oci_llm = init_oci_llm(llm_conf)
        kwargs = dict(model=oci_llm, tools=agent_tools, prompt=dynamic_prompt_callback)
        if checkpointer:
            self.agent = create_react_agent(checkpointer=checkpointer, **kwargs)
        else:
            self.agent = create_react_agent(**kwargs)

    async def invoke(self, user_query: str, **kwargs):
        # pre_tool_setup copies the session_variables kwarg into
        # chat_context.session_context_var; call it before reading any variable.
        token = pre_tool_setup(**kwargs)
        config = pre_invoke_setup(**kwargs)
        try:
            # Pattern 1 is also available here in invoke() (e.g. for routing):
            #   tenant = get_session_var("tenant_id")
            message = {"messages": [dict(HumanMessage(content=user_query))]}
            return await self.agent.ainvoke(input=message, config=config)
        finally:
            post_tool_setup(token, kwargs=kwargs)


# --- Local test harness ------------------------------------------------------ #
# On the platform, session variables arrive from the request `metadata` field
# (see README). Locally you can pass them directly as the `session_variables`
# kwarg, shaped as {"sessionvariables.<name>": {"name": ..., "value": ...}}.
import asyncio


async def main():
    agent = SessionVariableAgent()
    agent.setup()
    # Mirrors the shape the runtime stores: {full-key: {"name": full-key, "value": str}}.
    # Only "value" is read, so "name" is informational.
    session_variables = {
        f"sessionvariables.{n}": {"name": f"sessionvariables.{n}", "value": v}
        for n, v in {
            "user_id": "U_102",
            "tenant_id": "acme",
            "topic_name": "Kubernetes autoscaling",
        }.items()
    }
    result = await agent.invoke(
        "Generate my blog titles.",
        thread_id="local-test-thread",
        session_variables=session_variables,
    )
    result["messages"][-1].pretty_print()


if __name__ == "__main__":
    asyncio.run(main())
