"""Call limit middleware."""

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
    """Wrapper that owns two child middlewares."""


class CallLimitMiddlewareFactory(Middleware):
    """Produce model and tool call limit middlewares."""

    name = "call_limit"

    def __init__(
        self,
        *,
        model_run_limit: int | None = 40,
        tool_run_limit: int | None = 80,
        exit_behavior: str = "end",
    ) -> None:
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
    """Produce ToolCallLimitMiddleware for per-tool caps."""

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
