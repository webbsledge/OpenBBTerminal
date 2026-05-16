"""``call_limit`` middleware — bounds model + tool calls per run."""

from __future__ import annotations

from typing import Any

from langchain.agents.middleware import (
    ModelCallLimitMiddleware,
    ToolCallLimitMiddleware,
)
from langchain.agents.middleware.types import AgentMiddleware

from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import Middleware


class _Composite(AgentMiddleware):
    """Tiny wrapper that just owns two child middlewares."""


class CallLimitMiddlewareFactory(Middleware):
    """Produces ``ModelCallLimitMiddleware`` + ``ToolCallLimitMiddleware``."""

    name = "call_limit"

    def __init__(
        self,
        *,
        model_run_limit: int | None = 40,
        tool_run_limit: int | None = 80,
        exit_behavior: str = "end",
    ) -> None:
        # 40 model calls / 80 tool runs is enough headroom for
        # multi-step PDF research (search → get_pdf_outline →
        # several pdf_extract calls → final answer) without leaving
        # the door wide open for runaway loops. Override per-profile
        # via ``[agent.middleware_config.call_limit]`` if needed.
        self._model_run_limit = model_run_limit
        self._tool_run_limit = tool_run_limit
        self._exit_behavior = exit_behavior

    def build(self, ctx: RunContext, config: dict[str, Any]) -> AgentMiddleware:
        from typing import cast

        return cast(
            AgentMiddleware,
            ModelCallLimitMiddleware(
                run_limit=int(config.get("model_run_limit", self._model_run_limit)),
                exit_behavior=config.get("exit_behavior", self._exit_behavior),
            ),
        )


class ToolCallLimitMiddlewareFactory(Middleware):
    """Produces langchain's ``ToolCallLimitMiddleware`` for per-tool caps."""

    name = "tool_call_limit"

    def __init__(
        self,
        *,
        tool_run_limit: int | None = 80,
        exit_behavior: str = "end",
    ) -> None:
        self._tool_run_limit = tool_run_limit
        self._exit_behavior = exit_behavior

    def build(self, ctx: RunContext, config: dict[str, Any]) -> AgentMiddleware:
        from typing import cast

        return cast(
            AgentMiddleware,
            ToolCallLimitMiddleware(
                run_limit=int(config.get("tool_run_limit", self._tool_run_limit)),
                exit_behavior=config.get("exit_behavior", self._exit_behavior),
            ),
        )
