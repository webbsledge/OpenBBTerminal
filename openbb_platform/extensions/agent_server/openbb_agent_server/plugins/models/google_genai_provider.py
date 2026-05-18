"""Google Gemini model provider."""

from __future__ import annotations

from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from openbb_agent_server.plugins.models._validation import check_min, check_range
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ModelProvider


class GoogleGenAIProvider(ModelProvider):
    """Wrap ChatGoogleGenerativeAI."""

    name = "google_genai"

    def __init__(
        self,
        *,
        model_name: str = "gemini-2.5-flash",
        api_key: str | None = None,
        temperature: float = 0.0,
        max_output_tokens: int | None = None,
        top_p: float | None = None,
        top_k: int | None = None,
        timeout: float | None = None,
        max_retries: int = 6,
        seed: int | None = None,
        stop: list[str] | None = None,
        safety_settings: dict[Any, Any] | None = None,
        base_url: str | None = None,
        additional_headers: dict[str, str] | None = None,
        cached_content: str | None = None,
        response_mime_type: str | None = None,
        response_schema: dict[str, Any] | None = None,
        thinking_budget: int | None = None,
        thinking_level: str | None = None,
        include_thoughts: bool | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        check_range("temperature", temperature, 0.0, 2.0)
        check_range("top_p", top_p, 0.0, 1.0)
        check_min("top_k", top_k, 1)
        check_min("max_output_tokens", max_output_tokens, 1)
        check_min("max_retries", max_retries, 0)
        check_min("thinking_budget", thinking_budget, 0)
        if thinking_level is not None and thinking_level not in {
            "minimal",
            "low",
            "medium",
            "high",
        }:
            raise ValueError(
                "thinking_level must be one of 'minimal', 'low', 'medium', 'high'"
            )

        self._model_name = model_name
        self._api_key = api_key
        self._temperature = temperature
        self._max_output_tokens = max_output_tokens
        self._top_p = top_p
        self._top_k = top_k
        self._timeout = timeout
        self._max_retries = max_retries
        self._seed = seed
        self._stop = stop
        self._safety_settings = safety_settings
        self._base_url = base_url
        self._additional_headers = additional_headers
        self._cached_content = cached_content
        self._response_mime_type = response_mime_type
        self._response_schema = response_schema
        self._thinking_budget = thinking_budget
        self._thinking_level = thinking_level
        self._include_thoughts = include_thoughts
        self._labels = labels

    def build(  # noqa: PLR0912 — orchestrates many independent kwargs.
        self, ctx: RunContext, config: dict[str, Any]
    ) -> BaseChatModel:
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
        except ImportError as exc:  # pragma: no cover — install-hint path
            raise RuntimeError(
                "GoogleGenAIProvider requires langchain-google-genai. "
                "Install the agent_server with the [google_genai] extra."
            ) from exc

        api_key = (
            ctx.api_keys.get("GOOGLE_API_KEY")
            or ctx.api_keys.get("GEMINI_API_KEY")
            or self._api_key
        )

        kwargs: dict[str, Any] = {
            "model": config.get("model_name", self._model_name),
            "temperature": self._temperature,
            "max_retries": self._max_retries,
        }
        if api_key:
            kwargs["google_api_key"] = api_key
        if self._max_output_tokens is not None:
            kwargs["max_output_tokens"] = self._max_output_tokens
        if self._top_p is not None:
            kwargs["top_p"] = self._top_p
        if self._top_k is not None:
            kwargs["top_k"] = self._top_k
        if self._timeout is not None:
            kwargs["timeout"] = self._timeout
        if self._seed is not None:
            kwargs["seed"] = self._seed
        if self._stop is not None:
            kwargs["stop"] = list(self._stop)
        if self._safety_settings is not None:
            kwargs["safety_settings"] = self._safety_settings
        if self._base_url is not None:
            kwargs["base_url"] = self._base_url
        if self._additional_headers is not None:
            kwargs["additional_headers"] = dict(self._additional_headers)
        if self._cached_content is not None:
            kwargs["cached_content"] = self._cached_content
        if self._response_mime_type is not None:
            kwargs["response_mime_type"] = self._response_mime_type
        if self._response_schema is not None:
            kwargs["response_schema"] = self._response_schema
        if self._thinking_budget is not None:
            kwargs["thinking_budget"] = self._thinking_budget
        if self._thinking_level is not None:
            kwargs["thinking_level"] = self._thinking_level
        if self._include_thoughts is not None:
            kwargs["include_thoughts"] = self._include_thoughts
        if self._labels is not None:
            kwargs["labels"] = dict(self._labels)
        return ChatGoogleGenerativeAI(**kwargs)
