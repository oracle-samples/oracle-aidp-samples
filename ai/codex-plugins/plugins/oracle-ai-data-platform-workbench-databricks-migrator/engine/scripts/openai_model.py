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
_EXPLICIT_API_KEY: Optional[str] = None


def openai_client() -> Any:
    """Return a cached OpenAI client, failing early with clear setup messages."""
    global _CLIENT
    if _CLIENT is None:
        key = _EXPLICIT_API_KEY or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError(
                "OPENAI_API_KEY is not set. Export your OpenAI API key before running the migrator."
            )
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError(
                "The openai Python SDK is not installed. Run: pip install -r requirements.txt"
            ) from exc
        _CLIENT = OpenAI(api_key=key)
    return _CLIENT


def _usage_value(usage: Any, name: str, default: int = 0) -> int:
    if usage is None:
        return default
    if isinstance(usage, dict):
        return int(usage.get(name, default) or default)
    return int(getattr(usage, name, default) or default)


def usage_dict(response: Any) -> Dict[str, int]:
    usage = getattr(response, "usage", None)
    details = getattr(usage, "input_tokens_details", None)
    total_in = _usage_value(usage, "input_tokens")
    cached = min(_usage_value(details, "cached_tokens"), total_in)
    return {
        # Disjoint meters, matching the Anthropic ledger semantics the engine
        # sums: OpenAI's input_tokens INCLUDES cached tokens, so uncached
        # input = total - cached (never double-count).
        "input": total_in - cached,
        "output": _usage_value(usage, "output_tokens"),
        "cached": cached,
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


_EFFORT_MAP = {"max": "high"}  # Anthropic-side 'max' has no Responses equivalent


def _clamp_effort(effort: str) -> str:
    return _EFFORT_MAP.get(effort, effort)


def _response_create_kwargs(max_tokens: Optional[int] = None) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    if max_tokens:
        kwargs["max_output_tokens"] = max_tokens
    if OPENAI_REASONING_EFFORT:
        kwargs["reasoning"] = {"effort": _clamp_effort(OPENAI_REASONING_EFFORT)}
    return kwargs


def responses_create(
    *,
    instructions: Optional[str],
    input: Any,
    tools: Optional[List[Dict[str, Any]]] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[float] = None,
    effort: Optional[str] = None,
    model: Optional[str] = None,
) -> Any:
    client = openai_client()
    call_client = client.with_options(timeout=timeout) if timeout else client
    kwargs = _response_create_kwargs(max_tokens=max_tokens)
    if effort:
        kwargs["reasoning"] = {"effort": _clamp_effort(effort)}
    if tools is not None:
        kwargs["tools"] = tools
    # Stateless: nothing is retained provider-side as application state, and
    # reasoning items come back with encrypted content so they can be replayed
    # verbatim on the next round (required: a bare reasoning id cannot be
    # replayed when store=false).
    kwargs["store"] = False
    kwargs["include"] = ["reasoning.encrypted_content"]
    # Per-call model (= the schema-validated llm.model from YAML) is honored;
    # the OPENAI_MODEL env var remains an explicit operator override.
    return call_client.responses.create(
        model=os.environ.get("OPENAI_MODEL") or model or OPENAI_MODEL,
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


def _system_text(system: Any) -> Optional[str]:
    """v0.3.0 sends system as a LIST of text blocks carrying cache_control
    markers; OpenAI Responses takes one instructions string. Join the text and
    drop the markers (Responses manages caching server-side)."""
    if system is None or isinstance(system, str):
        return system
    if isinstance(system, list):
        parts = []
        for block in system:
            if isinstance(block, dict):
                parts.append(str(block.get("text", "")))
            else:
                parts.append(str(getattr(block, "text", block)))
        return "\n\n".join(p for p in parts if p)
    return str(system)


def _strip_cache_control(block: Any) -> Any:
    if isinstance(block, dict) and "cache_control" in block:
        return {k: v for k, v in block.items() if k != "cache_control"}
    return block


def _tool_result_text(content: Any) -> str:
    """tool_result content may be a plain string or a list of text blocks."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            str(b.get("text", "")) if isinstance(b, dict) else str(getattr(b, "text", b))
            for b in content
        )
    return str(content)


def _translate_error(exc: Exception) -> Exception:
    """Map OpenAI SDK transport errors onto the Anthropic exception types the
    engine's retry logic isinstance-checks, so transient handling keeps working
    under the openai provider. Status errors pass through: the 529/overloaded
    retry matches on the exception STRING, which repr()s fine either way."""
    try:
        import openai as _openai
        import anthropic as _anthropic
        import httpx as _httpx
    except ImportError:
        return exc
    req = getattr(exc, "request", None) or _httpx.Request("POST", "https://api.openai.com/v1/responses")
    if isinstance(exc, getattr(_openai, "APITimeoutError", ())):
        return _anthropic.APITimeoutError(request=req)
    if isinstance(exc, getattr(_openai, "APIConnectionError", ())):
        return _anthropic.APIConnectionError(message=str(exc), request=req)
    # Engine-visible markers: the emergency context trim keys on the Anthropic
    # phrase "prompt is too long"; the patient backoff keys on "overloaded_error".
    # Append those markers to the translated message so the engine's handling
    # fires for the OpenAI equivalents (context overflow / 5xx).
    status = getattr(exc, "status_code", None)
    msg = str(exc)
    if status == 400 and ("context length" in msg or "context_length_exceeded" in msg
                          or "maximum context" in msg):
        resp = _httpx.Response(status_code=400, request=req)
        return _anthropic.BadRequestError(
            message=msg + " (prompt is too long)", response=resp,
            body={"error": {"message": msg + " (prompt is too long)"}})
    if status is not None and int(status) >= 500:
        resp = _httpx.Response(status_code=int(status), request=req)
        return _anthropic.InternalServerError(
            message=msg + " (overloaded_error)", response=resp,
            body={"error": {"message": msg + " (overloaded_error)"}})
    return exc


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
    # The v0.3.0 token ledger reads the Anthropic cache fields; OpenAI Responses
    # reports cached input under usage.input_tokens_details.cached_tokens, which
    # we surface as cache reads (writes have no OpenAI equivalent -> 0).
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0


@dataclass
class ContentBlock:
    type: str
    text: str = ""
    name: str = ""
    input: Optional[Dict[str, Any]] = None
    id: str = ""
    thinking: str = ""
    raw_item: Any = None
    # raw Responses reasoning items that preceded this tool call; replayed
    # verbatim before the function_call item on the next round.
    preceding_items: tuple = ()
    # True on the 2nd..nth text part of one native message: the FIRST part's
    # raw_item replays the whole message, so these parts must not re-emit it.
    covered_by_raw: bool = False


class CompatResponse:
    def __init__(self, response: Any):
        self._response = response
        usage = usage_dict(response)
        self.usage = Usage(input_tokens=usage["input"], output_tokens=usage["output"],
                           cache_read_input_tokens=usage.get("cached", 0))
        self.content = self._content_blocks(response)
        if getattr(response, "status", None) == "incomplete":
            # Responses truncated output (e.g. max_output_tokens): surface the
            # Anthropic stop reason the engine's completion guard checks, so a
            # partial reply is never accepted as a complete one.
            self.stop_reason = "max_tokens"
        elif any(b.type == "tool_use" for b in self.content):
            self.stop_reason = "tool_use"
        else:
            self.stop_reason = "end_turn"

    @staticmethod
    def _content_blocks(response: Any) -> List[ContentBlock]:
        blocks: List[ContentBlock] = []
        pending_reasoning: List[Any] = []
        for item in getattr(response, "output", []) or []:
            item_type = getattr(item, "type", None)
            if item_type == "reasoning":
                # Reasoning items must be replayed verbatim alongside the item
                # they precede (Responses function-calling contract). Carried ON
                # the next tool_use block — never as a leading block of its own,
                # so content[0].text stays the model's text for single-shot
                # callers, and the engine's thinking-block stripping cannot
                # split a reasoning/function_call pair.
                pending_reasoning.append(item)
                continue
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
                        preceding_items=tuple(pending_reasoning),
                    )
                )
                pending_reasoning = []
            elif item_type == "message":
                first_part = True
                for content in getattr(item, "content", []) or []:
                    ctype = getattr(content, "type", None)
                    if ctype in ("output_text", "text"):
                        # The FIRST text part carries the raw message item
                        # (id/phase intact, replayed exactly once, covering ALL
                        # its parts) plus any reasoning that preceded it; later
                        # parts keep the text facade but are replay-suppressed.
                        blocks.append(ContentBlock(type="text",
                                                   text=getattr(content, "text", "") or "",
                                                   raw_item=item if first_part else None,
                                                   preceding_items=tuple(pending_reasoning) if first_part else (),
                                                   covered_by_raw=not first_part))
                        if first_part:
                            pending_reasoning = []
                            first_part = False
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
                        if block.covered_by_raw:
                            continue  # this part replays inside its message's raw item
                        for r in block.preceding_items:
                            converted.append(r)  # raw reasoning item, verbatim
                        if block.raw_item is not None:
                            converted.append(block.raw_item)  # raw message item (id/phase intact)
                        else:
                            converted.append({"type": "output_text", "text": block.text})
                    elif block.type == "tool_use":
                        for r in block.preceding_items:
                            converted.append(r)  # raw reasoning item, verbatim (encrypted content)
                        if block.raw_item is not None:
                            converted.append(block.raw_item)  # raw function_call item, ids intact
                        else:
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
                            "output": _tool_result_text(block.get("content", "")),
                        }
                    )
                elif isinstance(block, dict) and block.get("type") == "text":
                    converted.append({"type": "output_text", "text": str(block.get("text", ""))})
                elif isinstance(block, dict) and block.get("type") in ("thinking", "redacted_thinking"):
                    continue  # provider-internal; never replayed
                else:
                    converted.append(_strip_cache_control(block))
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
                    elif not isinstance(item, dict) and getattr(item, "type", None) in ("reasoning", "function_call", "message"):
                        converted.append(item)  # raw Responses output item, replayed verbatim
                    elif isinstance(item, dict) and item.get("type") in ("output_text", "text"):
                        converted.append({"role": "assistant", "content": item.get("text", "")})
                    else:
                        converted.append({"role": "assistant", "content": str(item)})
            elif role == "user" and isinstance(content, list):
                for item in cls._convert_content(content):
                    if isinstance(item, dict) and item.get("type") == "function_call_output":
                        converted.append(item)
                    elif isinstance(item, dict) and item.get("type") in ("output_text", "text"):
                        converted.append({"role": "user", "content": str(item.get("text", ""))})
                    else:
                        converted.append({"role": "user", "content": str(item)})
            else:
                converted.append({"role": role or "user", "content": cls._convert_content(content)})
        return converted

    def create(self, *, model: str, max_tokens: Optional[int] = None, timeout: Optional[float] = None,
               system: Any = None, messages: Optional[List[Dict[str, Any]]] = None,
               tools: Optional[List[Dict[str, Any]]] = None,
               output_config: Optional[Dict[str, Any]] = None, **_: Any) -> CompatResponse:
        # `thinking`, `tool_choice` and cache_control markers arrive via **_ /
        # block fields and are intentionally absorbed: Responses reasons by
        # default, and output_config.effort maps onto reasoning effort below.
        effort = (output_config or {}).get("effort")
        try:
            response = responses_create(
                instructions=_system_text(system),
                input=self._convert_messages(messages or []),
                tools=openai_tools_from_legacy(tools),
                max_tokens=max_tokens,
                timeout=timeout,
                effort=effort,
                model=model,
            )
        except Exception as exc:  # translate transport errors for the engine's retry logic
            raise _translate_error(exc) from exc
        return CompatResponse(response)

    def stream(self, **kwargs: Any) -> CompatStream:
        return CompatStream(self.create(**kwargs))


class OpenAIModelCompatClient:
    """Small compatibility facade for legacy messages.* call sites."""

    def __init__(self, api_key: Optional[str] = None):
        # Never mutate the process environment: an Anthropic key passed by a
        # legacy call site must not overwrite OPENAI_API_KEY. The explicit key
        # (when given) is handed to the OpenAI client directly.
        global _EXPLICIT_API_KEY
        _EXPLICIT_API_KEY = api_key or None
        self.messages = CompatMessages()
