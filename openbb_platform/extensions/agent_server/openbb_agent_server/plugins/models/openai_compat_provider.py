"""OpenAI-compatible-endpoint provider (optional install: ``[openai]``).

Targets any server that speaks the OpenAI Chat Completions wire
protocol — vLLM, NIM, TGI, Ollama, LM Studio, etc. Exposes the full
parameter surface those backends typically accept, including the
``reasoning_effort`` / ``reasoning_budget`` fields that NIM-class
servers add for reasoning models.
"""

from __future__ import annotations

from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from openbb_agent_server.plugins.models._validation import check_min, check_range
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ModelProvider

_LOCAL_API_KEY_PLACEHOLDER = "EMPTY"
_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high"}


class OpenAICompatProvider(ModelProvider):
    """Wraps :class:`langchain_openai.ChatOpenAI` against an arbitrary
    OpenAI-compatible endpoint.
    """

    name = "openai_compat"

    def __init__(
        self,
        *,
        # ── endpoint identity ─────────────────────────────────────
        # ``base_url``: required — points at the compat server's
        # ``/v1`` root. No default because there's nothing reasonable
        # to fall back to.
        base_url: str,
        # ``model_name``: required — exact name the backend serves.
        model_name: str,
        # ``api_key`` / ``organization``: come from runtime
        # ``ctx.api_keys`` if available; constructor kwargs are the
        # fallback. Local servers (Ollama, LM Studio) accept any
        # placeholder, so we send ``"EMPTY"`` if nothing was given.
        api_key: str | None = None,
        organization: str | None = None,
        # ── sampling (every OpenAI-compat backend) ────────────────
        # ``temperature`` in [0, 2]. Default 0.0 = greedy/deterministic
        # — best for tool-using agents.
        temperature: float = 0.0,
        # ``max_tokens``: ceiling on generated tokens. Default ``None``
        # = let the server pick.
        max_tokens: int | None = None,
        # ``top_p`` in [0, 1]: nucleus sampling. Default ``None`` =
        # server default (usually 1.0).
        top_p: float | None = None,
        # ``presence_penalty`` / ``frequency_penalty`` in [-2, 2].
        # Positive values discourage repetition. Default ``None`` =
        # server default (0).
        presence_penalty: float | None = None,
        frequency_penalty: float | None = None,
        # ``seed``: integer for best-effort deterministic sampling.
        # Default ``None``.
        seed: int | None = None,
        # ``n``: number of completions per request. Tool-using agents
        # always want one. Default 1.
        n: int = 1,
        # ``stop``: string or list of stop sequences. Default ``None``.
        stop: list[str] | str | None = None,
        # ── transport ─────────────────────────────────────────────
        # ``timeout`` (seconds) for the underlying HTTP call. Default
        # ``None`` = httpx default.
        timeout: float | None = None,
        # ``max_retries``: retries for transient HTTP errors (5xx /
        # rate-limit). Default 2.
        max_retries: int = 2,
        # ── reasoning (NIM / vLLM thinking models) ────────────────
        # ``reasoning_effort``: "none" | "minimal" | "low" | "medium"
        # | "high". Default ``None`` = let the server pick.
        reasoning_effort: str | None = None,
        # ``reasoning_budget``: max thinking tokens before the model
        # is forced to end its reasoning trace. ``-1`` disables
        # enforcement. Default ``None``.
        reasoning_budget: int | None = None,
        # ``chat_template_kwargs``: vLLM / NIM accept this top-level
        # field to forward kwargs into the chat-template render —
        # legacy path for ``reasoning_budget``.
        chat_template_kwargs: dict[str, Any] | None = None,
        # ── transport extras ──────────────────────────────────────
        # ``default_headers`` / ``default_query``: attached to every
        # request — useful for routing headers, API versioning.
        default_headers: dict[str, str] | None = None,
        default_query: dict[str, Any] | None = None,
        # ``streaming``: if True, partial message deltas are sent —
        # tokens stream back as data-only server-sent events (SSE)
        # as they become available (JSON responses are prefixed by
        # ``data:``), terminated by a ``data: [DONE]`` message. If
        # False, the response arrives as a single JSON object once
        # generation completes. Default True — the OpenBB agent
        # server's SSE protocol assumes streaming.
        streaming: bool = True,
        # ── escape hatches ────────────────────────────────────────
        # ``extra_body`` / ``model_kwargs``: arbitrary body-field
        # passthrough for backends that expose non-standard knobs.
        # Both merge into ``model_kwargs`` on ChatOpenAI;
        # ``model_kwargs`` wins on collision.
        extra_body: dict[str, Any] | None = None,
        model_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if not base_url:
            raise ValueError(
                "OpenAICompatProvider requires base_url (no default). Set "
                "[agent.model.config].base_url to your OpenAI-compatible server."
            )
        if not model_name:
            raise ValueError("OpenAICompatProvider requires model_name.")
        check_range("temperature", temperature, 0.0, 2.0)
        check_range("top_p", top_p, 0.0, 1.0)
        check_range("presence_penalty", presence_penalty, -2.0, 2.0)
        check_range("frequency_penalty", frequency_penalty, -2.0, 2.0)
        check_min("max_tokens", max_tokens, 1)
        check_min("n", n, 1)
        check_min("max_retries", max_retries, 0)
        if reasoning_effort is not None and reasoning_effort not in _REASONING_EFFORTS:
            raise ValueError(
                f"reasoning_effort must be one of {sorted(_REASONING_EFFORTS)}"
            )
        if reasoning_budget is not None and reasoning_budget < -1:
            raise ValueError(
                "reasoning_budget must be -1 (disabled) or >= 0 "
                f"(got {reasoning_budget})"
            )

        self._base_url = base_url
        self._model_name = model_name
        self._api_key = api_key
        self._organization = organization
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._top_p = top_p
        self._presence_penalty = presence_penalty
        self._frequency_penalty = frequency_penalty
        self._seed = seed
        self._n = n
        self._stop = stop
        self._timeout = timeout
        self._max_retries = max_retries
        self._reasoning_effort = reasoning_effort
        self._reasoning_budget = reasoning_budget
        self._chat_template_kwargs = chat_template_kwargs
        self._default_headers = default_headers
        self._default_query = default_query
        self._streaming = streaming
        self._extra_body = extra_body
        self._model_kwargs = model_kwargs

    def build(  # noqa: PLR0912 — orchestrates many independent kwargs.
        self, ctx: RunContext, config: dict[str, Any]
    ) -> BaseChatModel:
        try:
            from langchain_openai import ChatOpenAI
        except ImportError as exc:  # pragma: no cover — install-hint path
            raise RuntimeError(
                "OpenAICompatProvider requires langchain-openai. "
                "Install the agent_server with the [openai] extra."
            ) from exc

        api_key = (
            ctx.api_keys.get("OPENAI_COMPAT_API_KEY")
            or ctx.api_keys.get("OPENAI_API_KEY")
            or self._api_key
            or _LOCAL_API_KEY_PLACEHOLDER
        )

        kwargs: dict[str, Any] = {
            "model": config.get("model_name", self._model_name),
            "base_url": config.get("base_url", self._base_url),
            "api_key": api_key,
            "temperature": self._temperature,
            "n": self._n,
            "max_retries": self._max_retries,
            "streaming": self._streaming,
        }
        if self._organization is not None:
            kwargs["openai_organization"] = self._organization
        if self._max_tokens is not None:
            kwargs["max_tokens"] = self._max_tokens
        if self._top_p is not None:
            kwargs["top_p"] = self._top_p
        if self._presence_penalty is not None:
            kwargs["presence_penalty"] = self._presence_penalty
        if self._frequency_penalty is not None:
            kwargs["frequency_penalty"] = self._frequency_penalty
        if self._seed is not None:
            kwargs["seed"] = self._seed
        if self._stop is not None:
            kwargs["stop"] = self._stop
        if self._timeout is not None:
            kwargs["request_timeout"] = self._timeout
        if self._reasoning_effort is not None:
            # ``reasoning_effort`` is a native ChatOpenAI field on recent
            # langchain-openai versions; if the backend doesn't accept
            # it, it just gets ignored at the wire level.
            kwargs["reasoning_effort"] = self._reasoning_effort
        if self._default_headers is not None:
            kwargs["default_headers"] = dict(self._default_headers)
        if self._default_query is not None:
            kwargs["default_query"] = dict(self._default_query)

        # ``reasoning_budget`` / ``chat_template_kwargs`` are non-OpenAI
        # extensions exposed by vLLM, NIM, and TGI; spread them into
        # the request body via ``model_kwargs``.
        merged_model_kwargs: dict[str, Any] = {}
        if self._extra_body:
            merged_model_kwargs.update(self._extra_body)
        if self._reasoning_budget is not None:
            merged_model_kwargs["reasoning_budget"] = self._reasoning_budget
        if self._chat_template_kwargs is not None:
            merged_model_kwargs["chat_template_kwargs"] = dict(
                self._chat_template_kwargs
            )
        if self._model_kwargs:
            merged_model_kwargs.update(self._model_kwargs)
        if merged_model_kwargs:
            kwargs["model_kwargs"] = merged_model_kwargs
        return ChatOpenAI(**kwargs)
