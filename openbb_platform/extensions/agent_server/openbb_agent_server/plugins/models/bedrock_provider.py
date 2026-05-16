"""AWS Bedrock model provider (optional install: ``[bedrock]``)."""

from __future__ import annotations

from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from openbb_agent_server.plugins.models._validation import check_min, check_range
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ModelProvider


class BedrockProvider(ModelProvider):
    """Wraps :class:`langchain_aws.ChatBedrock`."""

    name = "bedrock"

    def __init__(
        self,
        *,
        model_name: str = "anthropic.claude-opus-4-7-v1:0",
        region_name: str | None = None,
        credentials_profile_name: str | None = None,
        endpoint_url: str | None = None,
        provider: str | None = None,
        temperature: float = 0.0,
        max_tokens: int | None = None,
        top_p: float | None = None,
        top_k: int | None = None,
        stop_sequences: list[str] | None = None,
        timeout: int | None = None,
        max_retries: int = 2,
        guardrails: dict[str, Any] | None = None,
        beta_use_converse_api: bool = False,
        streaming: bool = True,
    ) -> None:
        check_range("temperature", temperature, 0.0, 1.0)
        check_range("top_p", top_p, 0.0, 1.0)
        check_min("top_k", top_k, 1)
        check_min("max_tokens", max_tokens, 1)
        check_min("max_retries", max_retries, 0)

        self._model_name = model_name
        self._region_name = region_name
        self._credentials_profile_name = credentials_profile_name
        self._endpoint_url = endpoint_url
        self._provider = provider
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._top_p = top_p
        self._top_k = top_k
        self._stop_sequences = stop_sequences
        self._timeout = timeout
        self._max_retries = max_retries
        self._guardrails = guardrails
        self._beta_use_converse_api = beta_use_converse_api
        self._streaming = streaming

    def build(self, ctx: RunContext, config: dict[str, Any]) -> BaseChatModel:
        try:
            from langchain_aws import ChatBedrock
        except ImportError as exc:  # pragma: no cover — install-hint path
            raise RuntimeError(
                "BedrockProvider requires langchain-aws. "
                "Install the agent_server with the [bedrock] extra."
            ) from exc

        model_kwargs: dict[str, Any] = {}
        if self._top_p is not None:
            model_kwargs["top_p"] = self._top_p
        if self._top_k is not None:
            model_kwargs["top_k"] = self._top_k
        if self._stop_sequences is not None:
            model_kwargs["stop_sequences"] = list(self._stop_sequences)

        kwargs: dict[str, Any] = {
            "model_id": config.get("model_name", self._model_name),
            "temperature": self._temperature,
            "max_retries": self._max_retries,
            "streaming": self._streaming,
            "beta_use_converse_api": self._beta_use_converse_api,
        }
        if model_kwargs:
            kwargs["model_kwargs"] = model_kwargs
        if self._region_name is not None:
            kwargs["region_name"] = self._region_name
        if self._credentials_profile_name is not None:
            kwargs["credentials_profile_name"] = self._credentials_profile_name
        if self._endpoint_url is not None:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._provider is not None:
            kwargs["provider"] = self._provider
        if self._max_tokens is not None:
            kwargs["max_tokens"] = self._max_tokens
        if self._timeout is not None:
            kwargs["timeout"] = self._timeout
        if self._guardrails is not None:
            kwargs["guardrails"] = self._guardrails
        return ChatBedrock(**kwargs)
