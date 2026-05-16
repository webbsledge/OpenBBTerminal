"""``loop_guard`` middleware — short-circuits identical tool-call loops."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.messages import ToolMessage

from openbb_agent_server.runtime import emit
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import Middleware

logger = logging.getLogger("openbb_agent_server.middleware.loop_guard")


def _args_hash(args: Any) -> str:
    try:
        s = json.dumps(args, sort_keys=True, default=str)
    except (TypeError, ValueError):
        s = repr(args)
    return hashlib.sha256(s.encode()).hexdigest()[:12]


def _tool_call_field(request: Any, key: str) -> Any:
    tc = getattr(request, "tool_call", None)
    if tc is None:
        return None
    if isinstance(tc, dict):
        return tc.get(key)
    return getattr(tc, key, None)


def _tool_name(request: Any) -> str:
    return str(
        getattr(request, "tool_name", None)
        or _tool_call_field(request, "name")
        or "unknown"
    )


def _tool_args(request: Any) -> Any:
    return _tool_call_field(request, "args") or getattr(request, "args", None) or {}


def _tool_call_id(request: Any) -> str:
    return str(_tool_call_field(request, "id") or "")


class _LoopGuardMiddleware(AgentMiddleware):
    def __init__(self, *, max_repeats: int) -> None:
        self._max_repeats = max_repeats
        self._last_key: tuple[str, str] | None = None
        self._repeat_count: int = 0
        # Once tripped, every further identical call short-circuits
        # silently (no extra reasoning_step spam). Resets when the
        # model picks a different tool or different args.
        self._tripped: bool = False

    async def awrap_tool_call(self, request: Any, handler: Any) -> Any:
        from langgraph.errors import GraphBubbleUp

        name = _tool_name(request)
        key = (name, _args_hash(_tool_args(request)))

        if key == self._last_key:
            self._repeat_count += 1
        else:
            self._last_key = key
            self._repeat_count = 1
            self._tripped = False

        if self._repeat_count <= self._max_repeats:
            try:
                return await handler(request)
            except GraphBubbleUp:
                raise

        # The model is repeating the same (name, args) pair. Return a
        # synthetic ToolMessage telling it to stop instead of running
        # the tool again. The agent loop sees this as a normal
        # ToolMessage and continues — but with a message that the
        # model's training tells it to react to (i.e. stop calling).
        if not self._tripped:
            self._tripped = True
            emit.reasoning_step(
                f"Loop guard: {name!r} called "
                f"{self._repeat_count} times with identical arguments — "
                f"halting further calls and forcing the model to "
                f"answer with what it has.",
                event_type="WARNING",
                tool_name=name,
                repeat_count=self._repeat_count,
            )
            logger.warning(
                "loop_guard tripped: tool=%r repeats=%d", name, self._repeat_count
            )

        return ToolMessage(
            content=(
                f"[loop_guard] Tool {name!r} has been called "
                f"{self._repeat_count} times in a row with identical "
                "arguments and is returning the same result. STOP "
                "calling this tool. Answer the user with the "
                "information you already have, or try a different "
                "tool / different arguments."
            ),
            tool_call_id=_tool_call_id(request),
            name=name,
            # ``success`` (the default) — not ``error`` — so the
            # model treats this as a normal tool result with an
            # unusual instruction rather than a hard failure that
            # justifies giving up.
        )


class LoopGuardMiddlewareFactory(Middleware):
    """Build the per-run loop guard."""

    name = "loop_guard"

    def __init__(self, *, max_repeats: int = 2) -> None:
        # Two identical consecutive calls is the threshold — the
        # first is legitimate, the second is a plausible retry
        # after a parse glitch, the third+ is a loop and gets
        # short-circuited.
        self._max_repeats = max_repeats

    def build(self, ctx: RunContext, config: dict[str, Any]) -> AgentMiddleware:
        return _LoopGuardMiddleware(
            max_repeats=int(config.get("max_repeats", self._max_repeats))
        )
