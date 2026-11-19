"""Dual-version A2UI component gallery flow.

This flow emits and validates A2UI v0.9 or v0.8 operations according to the
client capabilities advertised by Agent Hub.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from aidputils.agents.toolkit.agent_helper import init_oci_llm, pre_invoke_setup
from aidputils.agents.toolkit.configs import OCIAIConf
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent

from a2ui_sdk.manager import A2uiValidator, A2uiSchemaManager
from a2ui_sdk.compat import (
    V08SurfaceBuilderAdapter,
    V08_UNSUPPORTED_COMPONENTS,
    advertised_catalog_ids,
    capability_candidates,
    catalog_id_matches_version,
)
from a2ui_sdk.parser import (
    build_text_message,
    extract_action_event,
    extract_response_text,
)
from templates.template_selection import TemplateSelectionService
from templates.template_utils import (
    COMPONENT_OPTIONS,
    SUPPORTED_COMPONENTS,
    SurfaceBuilder,
    action_ui_response,
    initial_ui_response,
    is_greeting_query,
    is_help_query,
    wants_ui,
)
logger = logging.getLogger("a2ui_component_demo_agent")

try:
  from deployment_config import COMPARTMENT_ID as PRIVATE_COMPARTMENT_ID
except ImportError:
  PRIVATE_COMPARTMENT_ID = None

########## Checkpointer creation #############
try:
    from aidputils.agents.toolkit.memory_helper import get_checkpoint_saver
    checkpointer = get_checkpoint_saver('a2ui_component_demo_agent')
except Exception:
    # The memory_helper script is not found in compute, use the checkpointer from globals instead.
    checkpointer = globals().get("checkpointer", None)
########## End Checkpointer creation #############

COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", PRIVATE_COMPARTMENT_ID or "<your-compartment-ocid>")
OCI_REGION = os.getenv("OCI_REGION", "us-ashburn-1")
OCI_ENDPOINT = os.getenv(
    "OCI_GENAI_ENDPOINT",
    f"https://inference.generativeai.{OCI_REGION}.oci.oraclecloud.com",
)
MODEL_ID = os.getenv("OCI_GENAI_MODEL_ID", "google.gemini-2.5-flash")

llm_conf = OCIAIConf(
    model_provider="generic",
    compartment_id=COMPARTMENT_ID,
    model_args={},
    endpoint=OCI_ENDPOINT,
    model_id=MODEL_ID,
    guardrails_config=None,
)

DEFAULT_COMPONENT = "Text"
SUPPORTED_COMPONENTS_CSV = ", ".join(SUPPORTED_COMPONENTS)
SUPPORTED_COMPONENTS_JSON_UNION = " | ".join(f'"{component}"' for component in SUPPORTED_COMPONENTS)

SCHEMA_MANAGER = A2uiSchemaManager(version="0.9")
SCHEMA_MANAGER_V08 = A2uiSchemaManager(version="0.8")
ROLE_DESCRIPTION = "You are a template selector for an A2UI component demo."
UI_DESCRIPTION = f"""
Allowed components: {SUPPORTED_COMPONENTS_CSV}.
Task: choose the best template component from the provided template catalog.

Do not generate A2UI JSON.
Output exactly one raw JSON object and nothing else:
{{
  "selectedComponent": {SUPPORTED_COMPONENTS_JSON_UNION}
}}
Rules:
- Never return any key other than selectedComponent.
- selectedComponent must be one of {SUPPORTED_COMPONENTS_CSV}.
- Only choose selectedComponent. The UI runtime will show all variants for the selectedComponent.
- If action context provides selectedComponent, prefer it unless user intent clearly requests switching component.
- If uncertain, choose selectedComponent=Text.
""".strip()
SYSTEM_PROMPT = SCHEMA_MANAGER.generate_system_prompt(
    role_description=ROLE_DESCRIPTION,
    workflow_description="",
    ui_description=UI_DESCRIPTION,
)

CHAT_SYSTEM_PROMPT = """
You are the friendly guide for the A2UI Component Gallery sample. Respond in
concise, natural plain text. You may chat normally and answer high-level
questions about A2UI and the component gallery. When useful, explain that the
user can say "show me a demo" to open the interactive gallery or ask to see a
specific component. Never emit raw A2UI JSON, claim to perform administrative
actions, or pretend that a component was rendered when it was not.
""".strip()

HELP_RESPONSE = (
    "I can chat with you about A2UI and show interactive examples of the "
    "supported components. Say \"show me a demo\" to open the component "
    "gallery, or ask for a specific example—such as \"show me the OADataGrid "
    "component\" or \"show me a chart.\""
)

GREETING_RESPONSE = (
    "Hi! I’m the A2UI Component Gallery guide. We can chat about A2UI, or you "
    "can say \"show me a demo\" to explore the interactive components."
)


def _capability_metadata(kwargs: dict[str, Any]) -> dict[str, Any] | None:
  """Read standard metadata, with AIDP session-variable transport fallback."""
  metadata = kwargs.get("metadata")
  if isinstance(metadata, dict) and "a2uiClientCapabilities" in metadata:
    return metadata
  variables = kwargs.get("session_variables") or {}
  if not isinstance(variables, dict):
    return metadata if isinstance(metadata, dict) else None
  for key in ("a2uiClientCapabilities", "sessionvariables.a2uiClientCapabilities"):
    value = variables.get(key)
    if isinstance(value, dict) and "value" in value:
      value = value["value"]
    if isinstance(value, str):
      try:
        value = json.loads(value)
      except json.JSONDecodeError:
        continue
    if isinstance(value, dict):
      return {"a2uiClientCapabilities": value}
  return metadata if isinstance(metadata, dict) else None


def _explicitly_lacks_a2ui(metadata: object) -> bool:
  """Return True only when a client explicitly sends empty A2UI capabilities.

  Metadata-free Agent Hub playground calls retain the historical behavior and
  receive A2UI. Other clients can opt into the text fallback by sending an
  empty ``a2uiClientCapabilities`` object.
  """
  if not isinstance(metadata, dict) or "a2uiClientCapabilities" not in metadata:
    return False
  caps = metadata.get("a2uiClientCapabilities")
  if not isinstance(caps, dict) or not caps or caps.get("enabled") is False:
    return True
  if "supportedCatalogIds" in caps and not caps.get("supportedCatalogIds"):
    return True
  return False


class A2UIComponentGallery:
  """Agent-flow entrypoint for the negotiated v0.8/v0.9 gallery."""

  def __init__(self) -> None:
    self.agent = None
    self.chat_agent = None
    self.schema_manager = SCHEMA_MANAGER
    self.schema_managers = {
        "0.9": SCHEMA_MANAGER,
        "0.8": SCHEMA_MANAGER_V08,
    }
    self.template_selection = TemplateSelectionService(
        supported_components=SUPPORTED_COMPONENTS,
        default_component=DEFAULT_COMPONENT,
    )
    self.surface_builder = SurfaceBuilder(
        template_selection=self.template_selection,
        component_options=COMPONENT_OPTIONS,
    )

  def setup(self) -> None:
    if self.agent is not None:
      return

    configured_compartment_id = (
        COMPARTMENT_ID.strip() if isinstance(COMPARTMENT_ID, str) else ""
    )
    if not configured_compartment_id or configured_compartment_id == "<your-compartment-ocid>":
      raise ValueError(
          "OCI compartment is not configured. Set OCI_COMPARTMENT_ID or copy "
          "deployment_config.example.py to deployment_config.py and replace "
          "<your-compartment-ocid>."
      )

    logger.info("Setting up dual-version A2UI template-selector agent...")
    oci_llm = init_oci_llm(llm_conf)

    try:
      self.agent = create_react_agent(
          model=oci_llm,
          tools=[],
          prompt=SYSTEM_PROMPT,
          debug=False,
          checkpointer=checkpointer if checkpointer else None,
      )
    except Exception as exc:
      logger.warning("Agent init with checkpointer failed: %s, retrying without", exc)
      self.agent = create_react_agent(
          model=oci_llm,
          tools=[],
          prompt=SYSTEM_PROMPT,
          debug=False,
      )

    try:
      self.chat_agent = create_react_agent(
          model=oci_llm,
          tools=[],
          prompt=CHAT_SYSTEM_PROMPT,
          debug=False,
      )
    except Exception as exc:
      logger.warning("Conversational agent init failed; using text fallback: %s", exc)

    logger.info("Dual-version A2UI template-selector agent ready.")

  def negotiate_a2ui(
      self,
      metadata: object,
  ) -> tuple[str, Any, str]:
    """Select the newest mutually supported protocol and catalog."""
    errors: list[str] = []
    for version, client_caps in capability_candidates(metadata):
      manager = self.schema_managers[version]
      try:
        catalog = manager.get_selected_catalog(client_ui_capabilities=client_caps)
        return version, catalog, catalog.catalog_id
      except ValueError as exc:
        errors.append(f"v{version}: {exc}")

      # The supplied v0.8 SDK used both a URL catalog ID and the legacy
      # agent-hub-catalog-v1-v08 alias. They describe the same bundled schema.
      # Preserve a known alias advertised by the client while validating
      # against the bundled catalog. Foreign IDs are never accepted.
      matching_ids = [
          catalog_id
          for catalog_id in advertised_catalog_ids(client_caps)
          if catalog_id_matches_version(catalog_id, version)
      ]
      if matching_ids:
        catalog = manager.get_selected_catalog()
        return version, catalog, matching_ids[0]

    raise ValueError("; ".join(errors) or "No supported A2UI protocol was advertised.")

  async def select_component_with_llm(
      self,
      user_query: str,
      action: dict[str, Any] | None,
      requested_component: str | None,
      config: dict[str, Any] | None = None,
  ) -> str:
    supported_components = self.template_selection.supported_components
    fallback_component = (
        requested_component
        if requested_component in supported_components
        else self.template_selection.default_component
    )

    if requested_component in supported_components:
      return requested_component

    if isinstance(action, dict):
      action_component = action.get("selectedComponent")
      if action_component in supported_components:
        return action_component

    deterministic_component = self.template_selection.deterministic_component_for_query(
        user_query
    )
    if deterministic_component in supported_components:
      return deterministic_component

    if self.agent is None:
      logger.warning("LLM agent is not initialized; using fallback component selection.")
      return fallback_component

    catalog_summary = self.template_selection.template_catalog_summary()
    intent = {
        "user_query": user_query,
        "is_action": action is not None,
        "action": action if isinstance(action, dict) else None,
        "requested_component": requested_component,
        "catalog": catalog_summary,
    }

    message = {
        "messages": [
            dict(
                HumanMessage(
                    content=(
                        "Select component based on this intent and catalog. "
                        "Return only the output JSON object.\n\n"
                        f"{json.dumps(intent, ensure_ascii=True)}"
                    )
                )
            )
        ]
    }

    try:
      result = await self.agent.ainvoke(input=message, config=config)
      content = extract_response_text(result)
      parsed = json.loads(content) if content else None
    except Exception as exc:
      logger.warning("Template selector LLM failed, using fallback component selection: %s", exc)
      parsed = None

    if not isinstance(parsed, dict):
      return fallback_component

    selected_component = parsed.get("selectedComponent")
    if selected_component not in supported_components:
      selected_component = fallback_component

    return selected_component

  async def invoke(self, user_query: str, **kwargs):
    config = pre_invoke_setup(**kwargs)
    if not isinstance(user_query, str):
      user_query = json.dumps(user_query, ensure_ascii=True, default=str)
    action = extract_action_event(user_query)
    if not isinstance(action, dict):
      if is_help_query(user_query):
        result = build_text_message(HELP_RESPONSE)
        return result

      if is_greeting_query(user_query):
        result = build_text_message(GREETING_RESPONSE)
        return result

      if not wants_ui(user_query):
        if self.chat_agent is not None:
          try:
            message = {"messages": [dict(HumanMessage(content=user_query))]}
            chat_result = await self.chat_agent.ainvoke(input=message, config=config)
            chat_text = extract_response_text(chat_result)
            if chat_text:
              result = build_text_message(chat_text)
              return result
          except Exception as exc:
            logger.warning("Conversational response failed; using help fallback: %s", exc)

        result = build_text_message(HELP_RESPONSE)
        return result

    # Capability negotiation belongs only to requests that will emit A2UI.
    # Ordinary chat must remain usable in text-only clients.
    metadata = _capability_metadata(kwargs)
    if _explicitly_lacks_a2ui(metadata):
      result = build_text_message(
          "This client does not advertise A2UI support, so I can’t render the "
          "interactive demo here. You can still ask me about A2UI components "
          "in text, or open this sample in an A2UI-capable Agent Hub client."
      )
      return result

    try:
      selected_version, selected_catalog, response_catalog_id = self.negotiate_a2ui(metadata)
    except ValueError as exc:
      logger.warning("No compatible A2UI protocol/catalog: %s", exc)
      result = build_text_message(
          "This client does not advertise a compatible A2UI v0.8 or v0.9 "
          "catalog. You can still ask me about the components in text."
      )
      return result
    selected_validator = A2uiValidator(selected_catalog)
    logger.info(
        "A2UI protocol negotiated: version=%s catalog_id=%s",
        selected_version,
        response_catalog_id,
    )

    invocation_template_selection = self.template_selection
    invocation_surface_builder: Any = self.surface_builder
    if selected_version == "0.8":
      v08_components = tuple(
          component
          for component in SUPPORTED_COMPONENTS
          if component not in V08_UNSUPPORTED_COMPONENTS
      )
      invocation_template_selection = TemplateSelectionService(
          supported_components=v08_components,
          default_component=DEFAULT_COMPONENT,
      )
      v08_options = [
          option
          for option in COMPONENT_OPTIONS
          if option.get("value") not in V08_UNSUPPORTED_COMPONENTS
      ]
      v09_builder = SurfaceBuilder(
          template_selection=invocation_template_selection,
          component_options=v08_options,
      )
      invocation_surface_builder = V08SurfaceBuilderAdapter(
          v09_builder,
          catalog_id=response_catalog_id,
          component_schemas=selected_catalog.catalog_schema["components"],
      )

    async def _select_for_invocation(
        query: str,
        action_payload: dict[str, Any] | None,
        requested: str | None,
    ) -> str:
      selected = await self.select_component_with_llm(
          query,
          action_payload,
          requested,
          config=config,
      )
      if selected not in invocation_template_selection.supported_components:
        return DEFAULT_COMPONENT
      return selected

    async def _repair_async(repair_prompt: str) -> str:
      if self.agent is None:
        raise ValueError("Repair callback requires initialized LLM agent.")
      repair_message = {"messages": [dict(HumanMessage(content=repair_prompt))]}
      repair_response = await self.agent.ainvoke(input=repair_message, config=config)
      return extract_response_text(repair_response)

    if isinstance(action, dict):
      try:
        result = await action_ui_response(
            template_selection=invocation_template_selection,
            surface_builder=invocation_surface_builder,
            user_query=user_query,
            action=action,
            select_component_with_llm=_select_for_invocation,
            catalog_id=response_catalog_id,
            schema_validate=lambda operations: selected_validator.validate(operations),
            repair_async=_repair_async if selected_version == "0.9" else None,
            repair_kwargs=kwargs,
        )
        return result
      except Exception as exc:
        logger.exception("Action response failed: %s", exc)
        result = build_text_message(
            "The requested component action could not be rendered. "
            "Show the component gallery again and retry the action."
        )
        return result

    if wants_ui(user_query):
      try:
        result = await initial_ui_response(
            template_selection=invocation_template_selection,
            select_component_with_llm=_select_for_invocation,
            user_query=user_query,
            surface_builder=invocation_surface_builder,
            catalog_id=response_catalog_id,
            schema_validate=lambda operations: selected_validator.validate(operations),
            repair_async=_repair_async if selected_version == "0.9" else None,
            repair_kwargs=kwargs,
        )
        return result
      except Exception as exc:
        logger.exception("Initial UI response failed: %s", exc)
        result = build_text_message(
            "The component gallery could not be rendered for this request. "
            "Try 'Show the component gallery' or name a supported component."
        )
        return result

    result = build_text_message(HELP_RESPONSE)
    return result


async def main():
  agent = A2UIComponentGallery()
  agent.setup()
  result = await agent.invoke("Show UI")
  print("\n-- Response --\n")
  print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
  asyncio.run(main())
