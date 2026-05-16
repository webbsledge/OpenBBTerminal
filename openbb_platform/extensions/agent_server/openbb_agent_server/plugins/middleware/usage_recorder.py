"""``usage_recorder`` middleware."""

from __future__ import annotations

import logging
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware

from openbb_agent_server.persistence.store import UsageRecord
from openbb_agent_server.runtime import (
    context as run_context,
    services,
)
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import Middleware

logger = logging.getLogger("openbb_agent_server.middleware.usage_recorder")


class _UsageRecorderMiddleware(AgentMiddleware):
    async def aafter_model(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        await self._record(state)
        return None

    async def _record(self, state: Any) -> None:
        messages = (state or {}).get("messages") or []
        if not messages:
            return
        last = messages[-1]
        usage = getattr(last, "usage_metadata", None)
        if not usage:
            return
        try:
            ctx: RunContext = run_context.current()
        except LookupError:
            logger.debug("usage_recorder: no RunContext bound, skipping")
            return
        details = usage.get("input_token_details", {}) or {}
        # Different langchain providers stash the model name in different
        # places; check the common ones in priority order.
        rmd = getattr(last, "response_metadata", {}) or {}
        addl = getattr(last, "additional_kwargs", {}) or {}
        model_name = (
            rmd.get("model")
            or rmd.get("model_name")
            or rmd.get("ls_model_name")
            or addl.get("model")
            or addl.get("model_name")
            or "unknown"
        )
        record = UsageRecord(
            trace_id=ctx.trace_id,
            user_id=ctx.principal.user_id,
            model=str(model_name),
            input_tokens=int(usage.get("input_tokens", 0)),
            output_tokens=int(usage.get("output_tokens", 0)),
            cache_read=int(details.get("cache_read", 0)),
            cache_creation=int(details.get("cache_creation", 0)),
            cost_usd=0.0,  # cost layered in by an opt-in pricing plugin
        )
        await services.get_history().record_usage(
            principal=ctx.principal,
            trace_id=ctx.trace_id,
            usage=record,
        )


class UsageRecorderMiddlewareFactory(Middleware):
    """Construct a :class:`_UsageRecorderMiddleware` for the current run."""

    name = "usage_recorder"

    def build(self, ctx: RunContext, config: dict[str, Any]) -> AgentMiddleware:
        return _UsageRecorderMiddleware()
