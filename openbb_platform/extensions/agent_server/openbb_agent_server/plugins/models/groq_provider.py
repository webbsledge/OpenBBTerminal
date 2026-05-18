"""Groq Cloud model provider."""

from __future__ import annotations

from typing import Any, Literal

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.rate_limiters import BaseRateLimiter

from openbb_agent_server.plugins.models._validation import check_min, check_range
from openbb_agent_server.plugins.models.groq_rate_limiter import (
    GROQ_LIMITS,
    GroqLimits,
    GroqRateLimiter,
    get_limiter,
)
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ModelProvider

_REASONING_EFFORTS = {"none", "low", "medium", "high", "default"}
_REASONING_FORMATS = {"parsed", "raw", "hidden"}
_SERVICE_TIERS: tuple[str, ...] = ("on_demand", "flex", "auto")


class GroqProvider(ModelProvider):
    """Wrap ChatGroq."""

    name = "groq"

    def __init__(
        self,
        *,
        model_name: str = "llama-3.3-70b-versatile",
        api_key: str | None = None,
        base_url: str | None = None,
        temperature: float = 0.0,
        max_tokens: int | None = None,
        top_p: float | None = None,
        n: int = 1,
        stop: list[str] | str | None = None,
        timeout: float | None = None,
        max_retries: int = 5,
        reasoning_effort: str | None = None,
        reasoning_format: Literal["parsed", "raw", "hidden"] | None = None,
        service_tier: Literal["on_demand", "flex", "auto"] = "on_demand",
        default_headers: dict[str, str] | None = None,
        streaming: bool = True,
        rate_limit: BaseRateLimiter | None = None,
        record_rate_limit_table: dict[str, GroqLimits | dict[str, int | None]]
        | None = None,
    ) -> None:
        check_range("temperature", temperature, 0.0, 2.0)
        check_range("top_p", top_p, 0.0, 1.0)
        check_min("max_tokens", max_tokens, 1)
        check_min("n", n, 1)
        check_min("max_retries", max_retries, 0)
        if reasoning_effort is not None and reasoning_effort not in _REASONING_EFFORTS:
            raise ValueError(
                f"reasoning_effort must be one of {sorted(_REASONING_EFFORTS)}"
            )
        if reasoning_format is not None and reasoning_format not in _REASONING_FORMATS:
            raise ValueError(
                f"reasoning_format must be one of {sorted(_REASONING_FORMATS)}"
            )
        if service_tier not in _SERVICE_TIERS:
            raise ValueError(f"service_tier must be one of {_SERVICE_TIERS}")

        self._model_name = model_name
        self._api_key = api_key
        self._base_url = base_url
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._top_p = top_p
        self._n = n
        self._stop = stop
        self._timeout = timeout
        self._max_retries = max_retries
        self._reasoning_effort = reasoning_effort
        self._reasoning_format = reasoning_format
        self._service_tier = service_tier
        self._default_headers = default_headers
        self._streaming = streaming
        self._rate_limit_override = rate_limit

        if record_rate_limit_table:
            for k, v in record_rate_limit_table.items():
                if isinstance(v, GroqLimits):
                    GROQ_LIMITS[k] = v
                elif isinstance(v, dict):
                    GROQ_LIMITS[k] = GroqLimits(**v)
                else:
                    raise ValueError(
                        f"record_rate_limit_table[{k!r}] must be a GroqLimits "
                        "instance or a kwargs dict for GroqLimits"
                    )

    def build(self, ctx: RunContext, config: dict[str, Any]) -> BaseChatModel:
        try:
            from langchain_groq import ChatGroq
        except ImportError as exc:  # pragma: no cover — install-hint path
            raise RuntimeError(
                "GroqProvider requires langchain-groq. "
                "Install the agent_server with the [groq] extra."
            ) from exc

        api_key = ctx.api_keys.get("GROQ_API_KEY") or self._api_key
        model_name = config.get("model_name", self._model_name)

        if self._rate_limit_override is not None:
            limiter = self._rate_limit_override
            usage_handler = (
                limiter.callback_handler
                if isinstance(limiter, GroqRateLimiter)
                else None
            )
        else:
            shared = get_limiter(api_key=api_key or "", model_name=model_name)
            limiter = shared
            usage_handler = shared.callback_handler

        callbacks = [usage_handler] if usage_handler is not None else []

        kwargs: dict[str, Any] = {
            "model": model_name,
            "temperature": self._temperature,
            "n": self._n,
            "max_retries": self._max_retries,
            "streaming": self._streaming,
            "service_tier": self._service_tier,
            "rate_limiter": limiter,
            "callbacks": callbacks,
        }
        if api_key:
            kwargs["api_key"] = api_key
        if self._base_url is not None:
            kwargs["base_url"] = self._base_url
        if self._max_tokens is not None:
            kwargs["max_tokens"] = self._max_tokens
        if self._top_p is not None:
            kwargs["model_kwargs"] = {"top_p": self._top_p}
        if self._stop is not None:
            kwargs["stop"] = self._stop
        if self._timeout is not None:
            kwargs["request_timeout"] = self._timeout
        if self._reasoning_effort is not None:
            kwargs["reasoning_effort"] = self._reasoning_effort
        if self._reasoning_format is not None:
            kwargs["reasoning_format"] = self._reasoning_format
        if self._default_headers is not None:
            kwargs["default_headers"] = dict(self._default_headers)
        return ChatGroq(**kwargs)
