# Copyright 2025 Google LLC
# Copyright 2026 Google LLC
# Modifications Copyright (C) 2026, Oracle and/or its affiliates.
#
# This file includes code adapted from the A2UI SDK and has been modified by Oracle.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# AIDP currently carries A2UI through a compact JSON representation of A2A
# parts. Keeping that wire shape as plain dictionaries avoids coupling this
# sample to a particular a2a-sdk release (the public SDK changed its Python
# models between releases).
def DataPart(*, data: Any, metadata: dict[str, Any]) -> dict[str, Any]:
  return {"kind": "data", "data": data, "metadata": metadata}


def TextPart(*, text: str) -> dict[str, Any]:
  return {"kind": "text", "text": text}


def Part(*, root: dict[str, Any]) -> dict[str, Any]:
  return {"root": root}

MIME_TYPE_KEY = "mimeType"
A2UI_MIME_TYPE = "application/json+a2ui"
A2UI_OPEN_TAG = "<a2ui-json>"
A2UI_CLOSE_TAG = "</a2ui-json>"

# ------------------------------------------------------------------------------
# A2UI ADK-derived parser helpers adapted from:
# - a2ui/a2a/parts.py
# - a2ui/parser/parser.py
# - a2ui/parser/payload_fixer.py
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# START OF PUBLIC PARSER FUNCTIONS
# ------------------------------------------------------------------------------
def create_a2ui_part(a2ui_data: dict[str, Any]) -> Part:
  """Creates an A2A Part containing A2UI data.

  Args:
      a2ui_data: The A2UI data dictionary.

  Returns:
      An A2A Part with a DataPart containing the A2UI data.
  """
  return Part(
      root=DataPart(
          data=a2ui_data,
          metadata={
              MIME_TYPE_KEY: A2UI_MIME_TYPE,
          },
      )
  )


def parse_and_fix(payload: str) -> List[Dict[str, Any]]:
  """Validates and applies autofixes to a raw JSON string and returns the parsed payload.

  Deprecated soon: use parse_json_payload(...) instead. The new name makes it
  clear this helper parses a JSON payload only and does not build agent
  responses.

  Args:
    payload: The raw JSON string from the LLM.

  Returns:
    A parsed and potentially fixed payload (list of dicts).
  """
  normalized_payload = _normalize_smart_quotes(payload)
  try:
    a2ui_json = _parse(normalized_payload)
    return a2ui_json
  except (
      json.JSONDecodeError,
      ValueError,
  ) as e:
    logger.warning(f"Initial A2UI payload validation failed: {e}")
    updated_payload = _remove_trailing_commas(normalized_payload)
    a2ui_json = _parse(updated_payload)
    return a2ui_json


def parse_json_payload(payload: str) -> List[Dict[str, Any]]:
  """Parses a raw JSON string and applies small LLM-output repairs."""
  return parse_and_fix(payload)


def parse_llm_text_to_a2a_parts(
    llm_text: str,
    validator: Optional[Any] = None,
) -> List[Part]:
  """Parses already-extracted LLM text into A2A parts."""
  return parse_response_to_a2a_parts(llm_text, validator=validator)


# ------------------------------------------------------------------------------
# END OF PUBLIC PARSER FUNCTIONS
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# START OF PRIVATE LOW-LEVEL PARSER FUNCTIONS
# ------------------------------------------------------------------------------
_A2UI_BLOCK_PATTERN = re.compile(
    f"{re.escape(A2UI_OPEN_TAG)}(.*?){re.escape(A2UI_CLOSE_TAG)}", re.DOTALL
)
JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
RepairAsyncInvoker = Callable[[str], Awaitable[str]]


@dataclass
class ResponsePart:
  """Represents a part of the LLM response.

  Attributes:
      text: The conversational text part. Can be an empty string.
      a2ui_json: The parsed A2UI JSON data, always a list of dictionaries if
        it contains A2UI messages. None if this part only contains trailing
        text.
  """

  text: str = ""
  a2ui_json: Optional[Any] = None


def _parse(payload: str) -> List[Dict[str, Any]]:
  """Parses the payload and returns a list of A2UI JSON objects."""
  try:
    a2ui_json = json.loads(payload)
    if not isinstance(a2ui_json, list):
      logger.info("Received a single JSON object, wrapping in a list for validation.")
      a2ui_json = [a2ui_json]
    return a2ui_json
  except json.JSONDecodeError as e:
    logger.error(f"Failed to parse JSON: {e}")
    raise ValueError(f"Failed to parse JSON: {e}")


def _normalize_smart_quotes(json_str: str) -> str:
  """Replaces smart (curly) quotes with standard straight quotes."""
  return (
      json_str.replace("\u201C", '"')
      .replace("\u201D", '"')
      .replace("\u2018", "'")
      .replace("\u2019", "'")
  )


def _remove_trailing_commas(json_str: str) -> str:
  """Attempts to remove trailing commas from a JSON string.

  Args:
    json_str: The raw JSON string from the LLM.

  Returns:
    A potentially fixed JSON string.
  """
  # Fix trailing commas: identifying commas followed by optional whitespace and a closing bracket (]) or brace (}).
  fixed_json = re.sub(r",(?=\s*[\]}])", "", json_str)

  if fixed_json != json_str:
    logger.warning("Detected trailing commas in LLM output; applied autofix.")

  return fixed_json


def _has_a2ui_parts(content: str) -> bool:
  """Checks if the content has A2UI parts."""
  return A2UI_OPEN_TAG in content and A2UI_CLOSE_TAG in content


def has_a2ui_parts(content: str) -> bool:
  """Checks if the content has A2UI parts.

  Deprecated soon: use parse_llm_text_to_a2a_parts(...) when handling LLM text.
  The new API validates tag balance and returns A2A parts instead of exposing
  tag-detection details to consumers.
  """
  return _has_a2ui_parts(content)


def _sanitize_json_string(json_string: str) -> str:
  """Sanitizes the JSON string by removing markdown code blocks."""
  json_string = json_string.strip()
  if json_string.startswith("```json"):
    json_string = json_string[len("```json") :]
  elif json_string.startswith("```"):
    json_string = json_string[len("```") :]
  if json_string.endswith("```"):
    json_string = json_string[: -len("```")]
  json_string = json_string.strip()
  return json_string


def _parse_response(content: str) -> List[ResponsePart]:
  """
  Parses the LLM response into a list of ResponsePart objects.

  Args:
      content: The raw LLM response.

  Returns:
      A list of ResponsePart objects.

  Raises:
      ValueError: If no A2UI tags are found or if the JSON part is invalid.
  """
  matches = list(_A2UI_BLOCK_PATTERN.finditer(content))

  if not matches:
    raise ValueError(
        f"A2UI tags '{A2UI_OPEN_TAG}' and '{A2UI_CLOSE_TAG}' not found in response."
    )

  response_parts = []
  last_end = 0

  for match in matches:
    start, end = match.span()
    # Text preceding the JSON block
    text_part = content[last_end:start].strip()

    # The JSON content within the tags
    json_string = match.group(1)
    json_string_cleaned = _sanitize_json_string(json_string)
    if not json_string_cleaned:
      raise ValueError("A2UI JSON part is empty.")

    json_data = parse_and_fix(json_string_cleaned)
    response_parts.append(ResponsePart(text=text_part, a2ui_json=json_data))
    last_end = end

  # Trailing text after the last JSON block
  trailing_text = content[last_end:].strip()
  if trailing_text:
    response_parts.append(ResponsePart(text=trailing_text, a2ui_json=None))

  return response_parts


def parse_response(content: str) -> List[ResponsePart]:
  """
  Parses the LLM response into a list of ResponsePart objects.

  Deprecated soon: use parse_llm_text_to_a2a_parts(...) instead. The new API is
  the public parser entrypoint for LLM text and keeps ResponsePart as an
  internal parser detail.

  Args:
      content: The raw LLM response.

  Returns:
      A list of ResponsePart objects.

  Raises:
      ValueError: If no A2UI tags are found or if the JSON part is invalid.
  """
  return _parse_response(content)


def _parse_response_to_parts(
    content: str,
    validator: Optional[Any] = None,
    fallback_text: Optional[str] = None,
) -> List[Part]:
  """Helper to parse LLM response content into A2A Parts, with optional validation.

  Args:
      content: The LLM response content, potentially containing A2UI delimiters.
      validator: Optional validator to run against extracted JSON payloads.
      fallback_text: Optional text to return if no parts are successfully created.

  Returns:
      A list of A2A Part objects (TextPart and/or DataPart).
  """
  parts = []
  response_parts = _parse_response(content)

  for part in response_parts:
    pending_parts: list[Part] = []
    if part.text:
      pending_parts.append(Part(root=TextPart(text=part.text)))

    if part.a2ui_json:
      json_data = part.a2ui_json
      if validator:
        _run_validator(validator, json_data)

      if isinstance(json_data, list):
        pending_parts.extend(create_a2ui_part(message) for message in json_data)
      else:
        pending_parts.append(create_a2ui_part(json_data))

    parts.extend(pending_parts)

  if not parts and fallback_text:
    parts.append(Part(root=TextPart(text=fallback_text)))

  return parts


def parse_response_to_parts(
    content: str,
    validator: Optional[Any] = None,
    fallback_text: Optional[str] = None,
) -> List[Part]:
  """Helper to parse LLM response content into A2A Parts, with optional validation.

  Deprecated soon: use parse_llm_text_to_a2a_parts(...) instead. The new API
  centralizes the LLM-text parsing contract, including plain text, wrapped A2UI,
  and unwrapped JSON validation.
  """
  return _parse_response_to_parts(
      content,
      validator=validator,
      fallback_text=fallback_text,
  )


def _run_validator(validator: Any, payload: Any) -> None:
  """Runs either a validator object (`.validate`) or a direct callable."""
  validate_method = getattr(validator, "validate", None)
  if callable(validate_method):
    validate_method(payload)
    return

  if callable(validator):
    validator(payload)
    return

  raise TypeError(
      "validator must be a callable or an object with a callable .validate(payload) method."
  )


def _looks_like_unwrapped_json(text: str) -> bool:
  normalized_text = _sanitize_json_string(text.strip())
  if not normalized_text:
    return False

  if normalized_text.startswith("{") or normalized_text.startswith("["):
    return True

  fenced = JSON_FENCE_RE.fullmatch(normalized_text)
  if not fenced:
    return False

  fenced_content = fenced.group(1).strip()
  return fenced_content.startswith("{") or fenced_content.startswith("[")


# ------------------------------------------------------------------------------
# END OF PRIVATE LOW-LEVEL PARSER FUNCTIONS
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# START OF AGENT RESPONSE BUILDER FUNCTIONS
# ------------------------------------------------------------------------------
def build_text_response(content: str) -> dict[str, Any]:
  return build_text_message(content)


def build_text_response_from_llm_text(
    llm_text: str,
    validator: Optional[Any] = None,
) -> dict[str, Any]:
  """Builds the current AIDP text response from already-extracted LLM text."""
  parts = parse_llm_text_to_a2a_parts(llm_text, validator=validator)
  return build_text_response_from_parts(parts)


async def build_text_response_from_llm_text_with_repair(
    llm_text: str,
    *,
    user_query: str,
    kwargs: Optional[dict[str, Any]] = None,
    validator: Optional[Any] = None,
    repair_async: Optional[RepairAsyncInvoker] = None,
    max_repair_attempts: int = 1,
) -> dict[str, Any]:
  """Parses/validates LLM text and optionally performs one async repair pass.

  Args:
      llm_text: Initial extracted LLM text to parse.
      user_query: Original user query used in the repair prompt.
      kwargs: Optional invocation kwargs used for repair prompt context.
      validator: Optional schema validator. Can be an object exposing
        `.validate(payload)` or a callable `(payload) -> None` (for example,
        `A2uiValidator(selected_catalog).validate`).
      repair_async: Optional async callback that receives repair prompt content
        and returns repaired LLM text.
      max_repair_attempts: Maximum number of repair attempts.
  """
  if max_repair_attempts < 0:
    raise ValueError("max_repair_attempts must be >= 0.")

  current_text = llm_text
  prompt_kwargs = kwargs or {}
  last_error: Optional[Exception] = None

  # +1 includes the initial parse of llm_text before any repair retries.
  for attempt in range(max_repair_attempts + 1):
    try:
      parts = parse_llm_text_to_a2a_parts(current_text, validator=validator)
      return build_text_response_from_parts(parts)
    except ValueError as error:
      last_error = error
      if repair_async is None or attempt >= max_repair_attempts:
        raise

      repair_prompt = build_repair_content(
          user_query=user_query,
          kwargs=prompt_kwargs,
          invalid_llm_text=current_text,
          validation_error=error,
      )
      repaired_text = await repair_async(repair_prompt)
      if not isinstance(repaired_text, str):
        raise ValueError("repair_async callback must return a string.")
      current_text = repaired_text

  if last_error is not None:
    raise last_error

  raise ValueError("Unable to parse LLM text.")


def build_text_response_from_parts(parts: list[Any]) -> dict[str, Any]:
  """Builds the current AIDP text response from A2A parts."""
  return build_text_response(build_a2ui_serialized_parts_text(parts))


def build_text_response_from_operations(
    operations: list[dict[str, Any]],
    text: str | None = None,
) -> dict[str, Any]:
  """Builds the current AIDP text response from raw A2UI operations."""
  parts: list[Any] = []
  if text:
    parts.append(Part(root=TextPart(text=text)))

  for operation in operations:
    parts.append(create_a2ui_part(operation))

  return build_text_response_from_parts(parts)


# ------------------------------------------------------------------------------
# END OF AGENT RESPONSE BUILDER FUNCTIONS
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# START OF AIDP TEXT RESPONSE WORKAROUND
# AIDP does not support native A2A TextPart/DataPart transport on this path yet.
# These helpers serialize structured parts through the outer `A2UI\n` text hack.
# ------------------------------------------------------------------------------
A2UI_TEXT_SENTINEL = "A2UI\n"


def build_text_message(content: str) -> dict[str, Any]:
  """Builds a LangChain AIMessage response.

  Deprecated soon: use build_text_response(...) instead. The new API names the
  response-building intent and keeps direct AIMessage construction out of agent
  consumers.
  """
  try:
    from langchain_core.messages import AIMessage
  except ImportError:
    # Agent Hub provides langchain-core at runtime. Keeping a plain message
    # fallback lets the schema/template test suite run in a clean environment
    # with only the sample-owned dependencies installed.
    message: Any = {"type": "ai", "content": content}
  else:
    message = AIMessage(content=content)
  return {"messages": [message]}


def build_a2ui_serialized_parts_text(parts: list[Any]) -> str:
  """Serializes A2A parts into the current AIDP text transport shape.

  Deprecated soon: use build_text_response_from_parts(...) instead. The new API
  owns the current A2UI text transport workaround so consumers do not assemble
  the A2UI sentinel string themselves.
  """
  return A2UI_TEXT_SENTINEL + serialize_a2a_parts(parts)


def serialize_a2a_parts(parts: list[Any]) -> str:
  """Serializes A2A Part-like objects to compact JSON.

  Deprecated soon: use build_text_response_from_parts(...) instead. The new API
  keeps serialization paired with response construction, which avoids consumers
  depending on the current transport internals.
  """
  serialized_parts = [_serialize_part_like(part) for part in parts]
  return json.dumps(serialized_parts, ensure_ascii=True, separators=(",", ":"))


def _build_serialized_parts_text(parts: list[Any]) -> str:
  return build_a2ui_serialized_parts_text(parts)


def _serialize_a2a_parts(parts: list[Any]) -> str:
  return serialize_a2a_parts(parts)


def _serialize_part_like(part: Any) -> dict[str, Any]:
  if isinstance(part, dict):
    return part

  model_dump = getattr(part, "model_dump", None)
  if callable(model_dump):
    try:
      return model_dump(mode="json", exclude_none=True)
    except TypeError:
      return model_dump(exclude_none=True)

  dict_dump = getattr(part, "dict", None)
  if callable(dict_dump):
    return dict_dump(exclude_none=True)

  root = getattr(part, "root", None)
  if root is None:
    raise TypeError(f"Unsupported A2A part object: {type(part)!r}")

  return {"root": _serialize_root_like(root)}


def _serialize_root_like(root: Any) -> dict[str, Any]:
  if isinstance(root, dict):
    return root

  model_dump = getattr(root, "model_dump", None)
  if callable(model_dump):
    try:
      return model_dump(mode="json", exclude_none=True)
    except TypeError:
      return model_dump(exclude_none=True)

  dict_dump = getattr(root, "dict", None)
  if callable(dict_dump):
    return dict_dump(exclude_none=True)

  kind = getattr(root, "kind", None)
  if kind == "text":
    return {
        "kind": "text",
        "text": getattr(root, "text", None),
    }

  if kind == "data":
    return {
        "kind": "data",
        "data": getattr(root, "data", None),
        "metadata": getattr(root, "metadata", None)
        or {MIME_TYPE_KEY: A2UI_MIME_TYPE},
    }

  raise TypeError(f"Unsupported A2A root object: {type(root)!r}")


def parse_response_to_a2a_parts(
    raw_text: str,
    validator: Optional[Any] = None,
) -> List[Part]:
  """Parses raw response text into A2A parts.

  Deprecated soon: use parse_llm_text_to_a2a_parts(...) instead. The new API
  names the expected input precisely as already-extracted LLM text and keeps
  response-object extraction separate.
  """
  text = raw_text.strip()
  if not text:
    raise ValueError("LLM returned an empty response.")

  if A2UI_OPEN_TAG in text or A2UI_CLOSE_TAG in text:
    if not has_a2ui_parts(text):
      raise ValueError(
          f"A2UI blocks must use balanced {A2UI_OPEN_TAG} and {A2UI_CLOSE_TAG} tags."
      )

  if has_a2ui_parts(text):
    parts = parse_response_to_parts(text, validator=validator)
    if not parts:
      raise ValueError("LLM response contained A2UI tags but could not be parsed.")
    return parts

  if _looks_like_unwrapped_json(text):
    raise ValueError(
        f"A2UI JSON must be wrapped in {A2UI_OPEN_TAG} and {A2UI_CLOSE_TAG}."
    )

  return [Part(root=TextPart(text=text))]


# ------------------------------------------------------------------------------
# END OF AIDP TEXT RESPONSE WORKAROUND
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# START OF AGENT HELPER FUNCTIONS
# ------------------------------------------------------------------------------
def extract_message_content_text(message: Any) -> str:
  """Extracts text content from one message-like object.

  Deprecated soon: use extract_agent_response_text(...) for agent responses. The
  new API describes the common consumer need and hides the message-content
  normalization details.
  """
  content = getattr(message, "content", None)
  if content is None and isinstance(message, dict):
    content = message.get("content")

  if isinstance(content, str):
    return content

  if isinstance(content, list):
    chunks: list[str] = []
    for item in content:
      if isinstance(item, str):
        chunks.append(item)
        continue

      if not isinstance(item, dict):
        text_value = getattr(item, "text", None)
        if isinstance(text_value, str):
          chunks.append(text_value)
        continue

      text_value = item.get("text")
      if isinstance(text_value, str):
        chunks.append(text_value)
    return "\n".join(chunk for chunk in chunks if chunk).strip()

  return str(content).strip()


def _extract_message_content_text(message: Any) -> str:
  return extract_message_content_text(message)


def extract_response_text(response: Any) -> str:
  """Extracts final text content from a LangGraph/agent response object.

  Deprecated soon: use extract_agent_response_text(...) instead. The new API
  names the expected source more clearly and avoids confusing response text with
  generic parser output.
  """
  if isinstance(response, dict):
    messages = response.get("messages")
    if isinstance(messages, list) and messages:
      return extract_message_content_text(messages[-1])

  return extract_message_content_text(response)


def extract_agent_response_text(agent_response: Any) -> str:
  """Extracts the final text content from a LangGraph/agent response object."""
  return extract_response_text(agent_response)


def extract_action_event(user_query: str) -> dict[str, Any] | None:
  text = user_query.strip()
  if not text:
    return None

  fenced = JSON_FENCE_RE.search(text)
  if fenced:
    text = fenced.group(1).strip()

  try:
    parsed = json.loads(text)
  except json.JSONDecodeError:
    return None

  if not isinstance(parsed, dict):
    return None

  user_action = parsed.get("userAction")
  if isinstance(user_action, dict):
    return user_action

  action = parsed.get("action")
  if not isinstance(action, dict):
    return None

  normalized = dict(action)
  if "surfaceId" not in normalized and isinstance(parsed.get("surfaceId"), str):
    normalized["surfaceId"] = parsed["surfaceId"]
  return normalized


def build_llm_content(user_query: str, kwargs: dict[str, Any]) -> str:
  """Builds the legacy prompt wrapper for an agent invocation.

  Deprecated soon: build the agent-specific human message content at the
  consumer call site instead. The parser should own parsing and response
  building, not compose prompt text for individual agents.
  """
  sections = [
      "Response format reminder:",
      "- Plain text is allowed.",
      f"- Each A2UI JSON block must be wrapped in {A2UI_OPEN_TAG} and {A2UI_CLOSE_TAG}.",
      "- Do not wrap A2UI JSON in markdown fences.",
      "- Do not return raw top-level JSON outside the A2UI tags.",
      f"User request:\n{user_query}",
  ]

  metadata = kwargs.get("metadata")
  if metadata is not None:
    sections.insert(
        0,
        "Invocation metadata JSON:\n"
        + json.dumps(metadata, ensure_ascii=True, default=str),
    )

  return "\n\n".join(sections)


def build_repair_content(
    user_query: str,
    kwargs: dict[str, Any],
    invalid_response: str | None = None,
    validation_error: Exception | None = None,
    *,
    invalid_llm_text: str | None = None,
) -> str:
  """Builds one repair prompt for invalid text plus A2UI output.

  Deprecated soon for the invalid_response= compatibility keyword: use
  invalid_llm_text= for new call sites. The new name clarifies that the repair
  prompt receives extracted LLM text, not the full agent response object.
  """
  invalid_text = invalid_response
  if invalid_text is None:
    invalid_text = invalid_llm_text
  if invalid_text is None:
    invalid_text = ""

  sections = [
      "The previous response was not valid for the text plus A2UI contract.",
      "Repair it and return plain text, one or more <a2ui-json>...</a2ui-json> blocks, or both.",
      f"Each A2UI JSON block must be wrapped in {A2UI_OPEN_TAG} and {A2UI_CLOSE_TAG}.",
      "Do not use markdown fences around A2UI JSON.",
      "- Do not return raw top-level JSON outside the A2UI tags.",
      f"Validation error:\n{validation_error}",
      f"Original user request:\n{user_query}",
  ]

  metadata = kwargs.get("metadata")
  if metadata is not None:
    sections.append(
        "Invocation metadata JSON:\n"
        + json.dumps(metadata, ensure_ascii=True, default=str)
    )

  sections.append(f"Invalid response to repair:\n{invalid_text}")
  return "\n\n".join(sections)


# ------------------------------------------------------------------------------
# END OF AGENT HELPER FUNCTIONS
# ------------------------------------------------------------------------------
