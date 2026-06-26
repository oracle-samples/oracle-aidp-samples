"""
OpenAI Responses API adapter for the AIDP migrator.

The original migrator was written around a Messages-style client. This module
keeps the rest of the migration engine stable while routing all model calls to
OpenAI Responses, including function/tool calls.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.5")
OPENAI_REASONING_EFFORT = os.environ.get("OPENAI_REASONING_EFFORT", "medium")

_CLIENT: Optional[Any] = None


def openai_client() -> Any:
    """Return a cached OpenAI client, failing early with clear setup messages."""
    global _CLIENT
    if _CLIENT is None:
        if not os.environ.get("OPENAI_API_KEY"):
            raise RuntimeError(
                "OPENAI_API_KEY is not set. Export your OpenAI API key before running the migrator."
            )
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError(
                "The openai Python SDK is not installed. Run: pip install -r requirements.txt"
            ) from exc
        _CLIENT = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return _CLIENT


def _usage_value(usage: Any, name: str, default: int = 0) -> int:
    if usage is None:
        return default
    if isinstance(usage, dict):
        return int(usage.get(name, default) or default)
    return int(getattr(usage, name, default) or default)


def usage_dict(response: Any) -> Dict[str, int]:
    usage = getattr(response, "usage", None)
    return {
        "input": _usage_value(usage, "input_tokens"),
        "output": _usage_value(usage, "output_tokens"),
    }


def response_text(response: Any) -> str:
    text = getattr(response, "output_text", None)
    if text:
        return text
    parts: List[str] = []
    for item in getattr(response, "output", []) or []:
        if getattr(item, "type", None) != "message":
            continue
        for content in getattr(item, "content", []) or []:
            ctype = getattr(content, "type", None)
            if ctype in ("output_text", "text"):
                parts.append(getattr(content, "text", "") or "")
    return "".join(parts)


def _response_create_kwargs(max_tokens: Optional[int] = None) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    if max_tokens:
        kwargs["max_output_tokens"] = max_tokens
    if OPENAI_REASONING_EFFORT:
        kwargs["reasoning"] = {"effort": OPENAI_REASONING_EFFORT}
    return kwargs


def responses_create(
    *,
    instructions: Optional[str],
    input: Any,
    tools: Optional[List[Dict[str, Any]]] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Any:
    client = openai_client()
    call_client = client.with_options(timeout=timeout) if timeout else client
    kwargs = _response_create_kwargs(max_tokens=max_tokens)
    if tools is not None:
        kwargs["tools"] = tools
    return call_client.responses.create(
        model=OPENAI_MODEL,
        instructions=instructions or None,
        input=input,
        **kwargs,
    )


def generate_text(
    system: str,
    user_content: str,
    *,
    max_tokens: Optional[int] = None,
    timeout: Optional[float] = 600,
) -> tuple[str, Dict[str, int]]:
    response = responses_create(
        instructions=system,
        input=[{"role": "user", "content": user_content}],
        max_tokens=max_tokens,
        timeout=timeout,
    )
    return response_text(response), usage_dict(response)


def openai_tools_from_legacy(tools: Optional[Iterable[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
    """Convert legacy tool definitions to OpenAI Responses function tools."""
    if not tools:
        return None
    converted: List[Dict[str, Any]] = []
    for tool in tools:
        converted.append(
            {
                "type": "function",
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema", {"type": "object", "properties": {}}),
                "strict": bool(tool.get("strict", False)),
            }
        )
    return converted


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    thinking_tokens: int = 0


@dataclass
class ContentBlock:
    type: str
    text: str = ""
    name: str = ""
    input: Optional[Dict[str, Any]] = None
    id: str = ""
    thinking: str = ""
    raw_item: Any = None


class CompatResponse:
    def __init__(self, response: Any):
        self._response = response
        usage = usage_dict(response)
        self.usage = Usage(input_tokens=usage["input"], output_tokens=usage["output"])
        self.content = self._content_blocks(response)
        self.stop_reason = "tool_use" if any(b.type == "tool_use" for b in self.content) else "end_turn"

    @staticmethod
    def _content_blocks(response: Any) -> List[ContentBlock]:
        blocks: List[ContentBlock] = []
        for item in getattr(response, "output", []) or []:
            item_type = getattr(item, "type", None)
            if item_type == "function_call":
                raw_args = getattr(item, "arguments", "{}") or "{}"
                try:
                    parsed = json.loads(raw_args)
                except json.JSONDecodeError:
                    parsed = {}
                blocks.append(
                    ContentBlock(
                        type="tool_use",
                        name=getattr(item, "name", ""),
                        input=parsed,
                        id=getattr(item, "call_id", None) or getattr(item, "id", ""),
                        raw_item=item,
                    )
                )
            elif item_type == "message":
                for content in getattr(item, "content", []) or []:
                    ctype = getattr(content, "type", None)
                    if ctype in ("output_text", "text"):
                        blocks.append(ContentBlock(type="text", text=getattr(content, "text", "") or ""))
        if not blocks:
            text = response_text(response)
            if text:
                blocks.append(ContentBlock(type="text", text=text))
        return blocks


class CompatStream:
    def __init__(self, response: CompatResponse):
        self._response = response
        self.text_stream = self._iter_text()

    def __enter__(self) -> "CompatStream":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def _iter_text(self):
        text = "".join(block.text for block in self._response.content if block.type == "text")
        if text:
            yield text

    def get_final_message(self) -> CompatResponse:
        return self._response


class CompatMessages:
    @staticmethod
    def _convert_content(content: Any) -> Any:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            converted: List[Any] = []
            for block in content:
                if isinstance(block, ContentBlock):
                    if block.type == "text":
                        converted.append({"type": "output_text", "text": block.text})
                    elif block.type == "tool_use":
                        converted.append(
                            {
                                "type": "function_call",
                                "call_id": block.id,
                                "name": block.name,
                                "arguments": json.dumps(block.input or {}),
                            }
                        )
                elif isinstance(block, dict) and block.get("type") == "tool_result":
                    converted.append(
                        {
                            "type": "function_call_output",
                            "call_id": block.get("tool_use_id"),
                            "output": block.get("content", ""),
                        }
                    )
                else:
                    converted.append(block)
            return converted
        return str(content)

    @classmethod
    def _convert_messages(cls, messages: List[Dict[str, Any]]) -> List[Any]:
        converted: List[Any] = []
        for message in messages:
            role = message.get("role")
            content = message.get("content")
            if role == "assistant" and isinstance(content, list):
                for item in cls._convert_content(content):
                    if isinstance(item, dict) and item.get("type") == "function_call":
                        converted.append(item)
                    elif isinstance(item, dict) and item.get("type") in ("output_text", "text"):
                        converted.append({"role": "assistant", "content": item.get("text", "")})
                    else:
                        converted.append({"role": "assistant", "content": str(item)})
            elif role == "user" and isinstance(content, list):
                for item in cls._convert_content(content):
                    if isinstance(item, dict) and item.get("type") == "function_call_output":
                        converted.append(item)
                    else:
                        converted.append({"role": "user", "content": str(item)})
            else:
                converted.append({"role": role or "user", "content": cls._convert_content(content)})
        return converted

    def create(self, *, model: str, max_tokens: Optional[int] = None, timeout: Optional[float] = None,
               system: Optional[str] = None, messages: Optional[List[Dict[str, Any]]] = None,
               tools: Optional[List[Dict[str, Any]]] = None, **_: Any) -> CompatResponse:
        response = responses_create(
            instructions=system,
            input=self._convert_messages(messages or []),
            tools=openai_tools_from_legacy(tools),
            max_tokens=max_tokens,
            timeout=timeout,
        )
        return CompatResponse(response)

    def stream(self, **kwargs: Any) -> CompatStream:
        return CompatStream(self.create(**kwargs))


class OpenAIModelCompatClient:
    """Small compatibility facade for legacy messages.* call sites."""

    def __init__(self, api_key: Optional[str] = None):
        if api_key:
            os.environ["OPENAI_API_KEY"] = api_key
        self.messages = CompatMessages()
