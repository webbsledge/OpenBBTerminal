"""``tool_message_normaliser`` middleware — strict human → assistant."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from openbb_agent_server.observability.logging import TRACE, trace
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import Middleware

logger = logging.getLogger("openbb_agent_server.middleware.tool_message_normaliser")


def _to_openai_tool_calls(tool_calls: Any) -> list[dict[str, Any]]:
    """Convert LangChain ``ToolCall`` list to OpenAI wire format.

    ``langchain_nvidia_ai_endpoints/_utils.py:convert_message_to_dict``
    only forwards ``additional_kwargs["tool_calls"]`` to the NIM wire,
    completely ignoring the modern top-level ``AIMessage.tool_calls``
    field. We mirror the calls into ``additional_kwargs`` in the
    OpenAI wire shape so the lib's serializer picks them up.
    """
    out: list[dict[str, Any]] = []
    for tc in tool_calls or []:
        if isinstance(tc, dict):
            name = str(tc.get("name") or "").strip()
            args = tc.get("args") or {}
            tc_id = str(tc.get("id") or "")
        else:
            name = str(getattr(tc, "name", "") or "").strip()
            args = getattr(tc, "args", None) or {}
            tc_id = str(getattr(tc, "id", "") or "")
        if not name:
            continue
        try:
            arguments_json = json.dumps(args, default=str)
        except (TypeError, ValueError):
            arguments_json = "{}"
        out.append(
            {
                "id": tc_id,
                "type": "function",
                "function": {"name": name, "arguments": arguments_json},
            }
        )
    return out


def _valid_tool_calls(tool_calls: Any) -> list[Any]:
    """Drop tool_calls with empty / missing ``name``.

    Streaming aggregators (notably langchain-nvidia and the NIM
    chunk parser) occasionally emit a stub tool_call entry with
    ``name=''`` before the real name token arrives, then never
    backfill it. If we forward that to Mistral Large 3, the
    server-side parser collapses the entire ``tool_calls`` list
    to ``None`` and the assistant message is rejected as
    ``content='' tool_calls=None``.
    """
    if not tool_calls:
        return []
    out: list[Any] = []
    for tc in tool_calls:
        if isinstance(tc, dict):
            name = str(tc.get("name") or "").strip()
        else:
            name = str(getattr(tc, "name", "") or "").strip()
        if not name:
            continue
        out.append(tc)
    return out


def _content_str(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        # ``content_blocks`` form — concatenate text blocks.
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif (
                isinstance(block, dict)
                and block.get("type") == "text"
                and isinstance(block.get("text"), str)
            ):
                parts.append(block["text"])
        return "".join(parts)
    return str(content)


def _strict_human_assistant(  # noqa: PLR0912
    messages: list[Any], *, preserve_tool_role: bool = False
) -> list[Any]:
    """Reduce the message list to strict human → assistant.

    ``preserve_tool_role=True`` keeps ``ToolMessage``s with their
    ``tool_call_id`` intact — used for providers that strictly require
    ``role: "tool"`` pairing (Mistral Large 3 specifically). The
    default (``False``) flattens them into a ``HumanMessage`` carrying
    the tool result as text, which is the behavior every other NIM
    model in the stack was tested against.
    """
    out: list[Any] = []
    pending_tool_payloads: list[tuple[str, str]] = []  # (tool_name, content)

    def _push_pending_as_human() -> None:
        nonlocal pending_tool_payloads
        if not pending_tool_payloads:
            return
        body = "\n\n".join(
            f"[tool: {n or 'tool'} result]\n{c}" for n, c in pending_tool_payloads
        )
        out.append(HumanMessage(content=body))
        pending_tool_payloads = []

    for m in messages:
        if isinstance(m, SystemMessage):
            out.append(m)
            continue
        if isinstance(m, ToolMessage):
            payload = _content_str(m.content).strip()
            if not payload:
                continue
            if preserve_tool_role:
                # Mistral path: emit the ToolMessage verbatim so the
                # serializer renders it as ``{role:"tool",
                # tool_call_id:..., content:...}`` and the strict
                # function-call/response pairing invariant holds.
                out.append(
                    ToolMessage(
                        content=payload,
                        tool_call_id=getattr(m, "tool_call_id", "") or "",
                        name=getattr(m, "name", None),
                    )
                )
                continue
            name = str(getattr(m, "name", "") or "")
            pending_tool_payloads.append((name, payload))
            continue
        if isinstance(m, AIMessage):
            # Flush any pending tool results AS A HUMAN TURN before
            # this AI turn so the model sees "(X result) → I now call Y".
            _push_pending_as_human()
            text = _content_str(m.content).strip()
            raw_tool_calls = _valid_tool_calls(getattr(m, "tool_calls", []) or [])
            if not text and not raw_tool_calls:
                continue
            # CRITICAL: keep the structured ``tool_calls`` field intact.
            # Earlier versions of this middleware appended a textual
            # ``"(called <tool>)"`` marker AND dropped the tool_calls
            # field — that poisoned the conversation history (the next
            # model invocation read the marker as plain text and
            # imitated the pattern, emitting ``"(called search_pdf)"``
            # in prose instead of producing a structured tool_call).
            # Preserve the structured field so the chat template /
            # downstream model gets a real tool-call signal.
            new_kwargs = dict(getattr(m, "additional_kwargs", {}) or {})
            # ``langchain_nvidia_ai_endpoints`` only forwards
            # ``additional_kwargs["tool_calls"]`` to the NIM wire (it
            # ignores the top-level ``AIMessage.tool_calls`` field).
            # Mirror our calls into additional_kwargs in OpenAI wire
            # shape so Mistral / Nemotron / etc receive them. Without
            # this, tool_calls vanish silently between the agent loop
            # and the NIM endpoint, and Mistral 400s with
            # ``Invalid assistant message: content='' tool_calls=None``.
            if raw_tool_calls:
                new_kwargs["tool_calls"] = _to_openai_tool_calls(raw_tool_calls)
            else:
                new_kwargs.pop("tool_calls", None)
            out.append(
                AIMessage(
                    content=text,
                    tool_calls=raw_tool_calls,
                    additional_kwargs=new_kwargs,
                )
            )
            continue
        if isinstance(m, HumanMessage):
            text = _content_str(m.content)
            if not text.strip() and not pending_tool_payloads:
                continue
            prefix_blocks = "\n\n".join(
                f"[tool: {n or 'tool'} result]\n{c}" for n, c in pending_tool_payloads
            )
            pending_tool_payloads = []
            combined = f"{prefix_blocks}\n\n{text}".strip() if prefix_blocks else text
            out.append(HumanMessage(content=combined))
            continue
        # Unknown message — treat content as user input.
        text = _content_str(getattr(m, "content", None)).strip()
        if text:
            _push_pending_as_human()
            out.append(HumanMessage(content=text))

    # Flush any trailing tool results so they aren't lost.
    _push_pending_as_human()

    # Pass 2 — merge consecutive same-role turns so the chat
    # template sees strict alternation. When merging two AIMessages
    # we keep the UNION of their structured ``tool_calls`` so the
    # model doesn't lose them.
    merged: list[Any] = []
    for m in out:
        # ``ToolMessage``s NEVER merge — each carries its own
        # ``tool_call_id`` paired to a distinct assistant tool_call,
        # and merging would both lose the pairing and crash the
        # constructor (``ToolMessage(content=...)`` without
        # tool_call_id raises ``KeyError: 'tool_call_id'``).
        if isinstance(m, ToolMessage):
            merged.append(m)
            continue
        if merged and not isinstance(m, SystemMessage) and type(merged[-1]) is type(m):
            prev = merged[-1]
            joined = (
                f"{_content_str(prev.content)}\n\n{_content_str(m.content)}".strip()
            )
            if isinstance(prev, AIMessage):
                merged_tool_calls = _valid_tool_calls(
                    list(getattr(prev, "tool_calls", []) or [])
                    + list(getattr(m, "tool_calls", []) or [])
                )
                merged_kwargs = dict(getattr(prev, "additional_kwargs", {}) or {})
                if merged_tool_calls:
                    merged_kwargs["tool_calls"] = _to_openai_tool_calls(
                        merged_tool_calls
                    )
                else:
                    merged_kwargs.pop("tool_calls", None)
                merged[-1] = AIMessage(
                    content=joined,
                    tool_calls=merged_tool_calls,
                    additional_kwargs=merged_kwargs,
                )
            else:
                merged[-1] = type(prev)(content=joined)
        else:
            merged.append(m)

    # Pass 3 — drop any AIMessage that's empty on BOTH axes (no text
    # content AND no tool_calls). Most providers tolerate empty
    # assistant turns silently, but Mistral Large 3 rejects them
    # with ``Invalid assistant message: content='' tool_calls=None``.
    # These show up in history when the model returns
    # ``finish_reason=stop`` with no body — e.g. when a previous turn
    # ran out of tokens or was short-circuited by ``call_limit`` /
    # ``loop_guard``.
    cleaned: list[Any] = []
    for m in merged:
        if isinstance(m, AIMessage):
            text_ok = bool(_content_str(m.content).strip())
            calls_ok = bool(_valid_tool_calls(getattr(m, "tool_calls", None) or []))
            if not text_ok and not calls_ok:
                # Unreachable in practice: pass 1 already drops every
                # AIMessage empty on both axes before append, and
                # pass 2's merge always unions valid ``tool_calls`` or
                # joins non-empty text — so ``merged`` never holds an
                # empty AIMessage. Kept as defense-in-depth for the
                # Mistral "no empty assistant turn" invariant.
                continue  # pragma: no cover
        cleaned.append(m)
    return cleaned


def _dedupe_tool_calls(response: Any) -> Any:
    """Collapse identical ``tool_calls`` in one AIMessage to a single call."""
    import json as _json

    if response is None:
        return response
    tool_calls = list(getattr(response, "tool_calls", []) or [])
    if len(tool_calls) <= 1:
        return response

    seen: set[tuple[str, str]] = set()
    unique: list[Any] = []
    for tc in tool_calls:
        if isinstance(tc, dict):
            name = str(tc.get("name", ""))
            args = tc.get("args", {})
        else:
            name = str(getattr(tc, "name", ""))
            args = getattr(tc, "args", {})
        try:
            args_key = _json.dumps(args, sort_keys=True, default=str)
        except (TypeError, ValueError):
            args_key = repr(args)
        key = (name, args_key)
        if key in seen:
            continue
        seen.add(key)
        unique.append(tc)

    if len(unique) == len(tool_calls):
        return response

    try:
        response.tool_calls = unique
    except (AttributeError, TypeError):
        # Immutable model — fall back to copying.
        from langchain_core.messages import AIMessage

        if isinstance(response, AIMessage):
            return AIMessage(
                content=response.content,
                tool_calls=unique,
                id=getattr(response, "id", None),
                additional_kwargs=getattr(response, "additional_kwargs", {}) or {},
            )
    return response


def _dump_messages(messages: list[Any], where: str) -> None:
    """Diagnostic — TRACE-log every message about to be sent to the model."""
    if not logger.isEnabledFor(TRACE):
        return
    for i, m in enumerate(messages):
        tc = getattr(m, "tool_calls", None)
        content = getattr(m, "content", None)
        cstr = content if isinstance(content, str) else repr(content)[:120]
        trace(
            logger,
            "%s msg[%d] %s content=%r tool_calls=%r",
            where,
            i,
            type(m).__name__,
            cstr[:200] if isinstance(cstr, str) else cstr,
            tc,
        )


def _wants_tool_role(request: Any) -> bool:
    """Return True when the request's model needs strict ``role:"tool"`` pairing.

    Some providers (Mistral Large 3) strictly require ``role:"tool"``
    responses paired by ``tool_call_id`` and 400 with ``Not the same
    number of function calls and responses`` if we flatten them into
    HumanMessages. Detect those by model-name substring.
    """
    model = getattr(request, "model", None)
    name = getattr(model, "model", None) or getattr(model, "model_name", None) or ""
    return "mistral" in str(name).lower()


class _ToolMessageNormaliserMiddleware(AgentMiddleware):
    def wrap_model_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
    ) -> Any:
        new_messages = _strict_human_assistant(
            list(request.messages),
            preserve_tool_role=_wants_tool_role(request),
        )
        _dump_messages(new_messages, "tool_message_normaliser -> model")
        response = handler(request.override(messages=new_messages))
        return _dedupe_tool_calls(response)

    async def awrap_model_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        new_messages = _strict_human_assistant(
            list(request.messages),
            preserve_tool_role=_wants_tool_role(request),
        )
        _dump_messages(new_messages, "tool_message_normaliser -> model")
        response = await handler(request.override(messages=new_messages))
        return _dedupe_tool_calls(response)


class ToolMessageNormaliserMiddlewareFactory(Middleware):
    """Build the per-run normaliser."""

    name = "tool_message_normaliser"

    def build(self, ctx: RunContext, config: dict[str, Any]) -> AgentMiddleware:
        return _ToolMessageNormaliserMiddleware()
