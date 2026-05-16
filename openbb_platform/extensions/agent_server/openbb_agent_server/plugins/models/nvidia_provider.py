"""NVIDIA NIM / NeMo / Foundation-Models provider (optional: ``[nvidia]``).

Mirrors the full ``Parameters`` panel that NIM exposes in the "View
Code" sidebar so every knob the user can drag in the UI can also be
set declaratively in a profile TOML. Per-model UIs surface different
subsets — reasoning models add ``reasoning_effort`` /
``reasoning_budget``, instruct models add penalty fields — but the
NIM Chat Completions endpoint accepts them all and ignores the ones
its backend doesn't use, so this provider exposes the union.
"""

from __future__ import annotations

from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from openbb_agent_server.plugins.models._validation import check_min, check_range
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ModelProvider

_REASONING_EFFORTS = {"none", "low", "medium", "high"}


def _silence_unknown_tools_warning(model: Any) -> None:
    """Flip the per-model ``supports_tools`` flag inside ChatNVIDIA.

    langchain-nvidia-ai-endpoints keeps a hardcoded allow-list of
    models it "knows" support tools and warns at ``bind_tools`` time
    for anything outside it (``"Model 'X' is not known to support
    tools. Your tool binding may fail at inference time."``). The list
    lags every new NIM release — Mistral Large 3, MiniMax M2,
    qwen3-thinking and friends all support tools via different
    formats, just not the OpenAI one the allow-list checks for. The
    warning is misleading noise, and module-level
    ``warnings.filterwarnings`` is fragile (pytest / app code can
    reset filters), so override the flag directly on the registry
    entry.
    """
    try:
        client = getattr(model, "_client", None)
        registry_entry = getattr(client, "model", None) if client else None
        if registry_entry is not None and hasattr(registry_entry, "supports_tools"):
            registry_entry.supports_tools = True
    except Exception:  # noqa: S110 — deliberate best-effort swallow
        # The registry-flag override is best-effort — if the upstream
        # shape changes, fall through silently. The worst case is the
        # warning we were trying to silence reappears; nothing breaks.
        pass


class NvidiaProvider(ModelProvider):
    """Wraps :class:`langchain_nvidia_ai_endpoints.ChatNVIDIA`.

    Native ChatNVIDIA fields (``temperature``, ``max_tokens``,
    ``top_p``, ``seed``, ``stop``, ``base_url``, ``default_headers``,
    ``disable_streaming``) feed directly into the constructor.
    Non-native fields (``reasoning_effort``, ``reasoning_budget``,
    ``frequency_penalty``, ``presence_penalty``,
    ``chat_template_kwargs``) ride along in ``model_kwargs``, which
    ChatNVIDIA spreads into the chat-completion request body. That
    avoids langchain's noisy "WARNING! X is not a default parameter,
    transferring to model_kwargs" auto-transfer.
    """

    name = "nvidia"

    def __init__(
        self,
        *,
        # ── identity ────────────────────────────────────────────
        # ``model_name``: the NIM model path (e.g.
        # ``deepseek-ai/deepseek-r1`` or ``meta/llama-3.3-70b-instruct``).
        # The default is a safe, broadly-available instruct model.
        model_name: str = "meta/llama-3.3-70b-instruct",
        # ``api_key`` / ``base_url`` come from runtime ``ctx.api_keys``
        # (``NVIDIA_API_KEY``) if present; the kwargs are the fallback
        # for static profile configs.
        api_key: str | None = None,
        base_url: str | None = None,
        # ── sampling (every chat model in the NIM catalogue) ───
        # ``temperature``: 0.0 = greedy / deterministic, 2.0 = wildest.
        # Default 0.0 because agentic / tool-using workflows benefit
        # from stable outputs; raise to ~0.7 for creative writing.
        temperature: float = 0.0,
        # ``max_tokens`` / ``max_completion_tokens``: ceiling on
        # generated tokens. NIM accepts either name (``max_tokens`` is
        # the legacy spelling; ``max_completion_tokens`` is what the
        # newest reasoning models expect). We treat them as aliases.
        # Default ``None`` lets the server pick its own ceiling.
        max_tokens: int | None = None,
        max_completion_tokens: int | None = None,
        # ``top_p``: nucleus-sampling truncation in [0, 1]. Default
        # ``None`` = server default (1.0 on most NIM models).
        top_p: float | None = None,
        # ``frequency_penalty`` / ``presence_penalty``: in [-2, 2].
        # Positive values discourage repetition. Default ``None``
        # leaves the server at 0 (no penalty).
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
        # ``seed``: integer for best-effort deterministic sampling.
        # Default ``None`` = no determinism.
        seed: int | None = None,
        # ``stop``: string or list of stop sequences. The returned
        # text omits the sequence. Default ``None`` = no stop.
        stop: list[str] | str | None = None,
        # ── streaming ───────────────────────────────────────────
        # ``streaming``: if True, partial message deltas are sent —
        # tokens stream back as data-only server-sent events (SSE)
        # as they become available (JSON responses are prefixed by
        # ``data:``), terminated by a ``data: [DONE]`` message. If
        # False, the response arrives as a single JSON object once
        # generation completes. Default True — the OpenBB agent
        # server's SSE protocol assumes streaming. ChatNVIDIA's
        # native field is ``disable_streaming`` (inverted); we
        # translate at build time.
        streaming: bool = True,
        # ── reasoning models only (deepseek-r1, qwen3-thinking, …) ──
        # ``reasoning_effort``: "none" | "low" | "medium" | "high".
        # Default ``None`` = let the server's per-model default apply.
        reasoning_effort: str | None = None,
        # ``reasoning_budget``: max tokens the model may spend on
        # internal thinking before being forced to end the trace.
        # ``-1`` disables enforcement. Default ``None`` = server
        # default. Most useful paired with ``reasoning_effort="high"``.
        reasoning_budget: int | None = None,
        # ``chat_template_kwargs``: legacy path for
        # ``reasoning_budget`` (some older NIM builds only read the
        # value here). Forward both so the backend picks what it
        # supports. Per NIM docs, the top-level field wins when both
        # are set.
        chat_template_kwargs: dict[str, Any] | None = None,
        # ── escape hatches ─────────────────────────────────────
        # ``default_headers``: extra HTTP headers to attach to every
        # request (e.g. ``X-Tenant`` for routing).
        default_headers: dict[str, str] | None = None,
        # ``extra_body`` / ``model_kwargs``: arbitrary body-field
        # passthrough for fields not modelled above. Both merge into
        # ``model_kwargs`` on ChatNVIDIA; ``model_kwargs`` wins on
        # collision.
        extra_body: dict[str, Any] | None = None,
        model_kwargs: dict[str, Any] | None = None,
    ) -> None:
        check_range("temperature", temperature, 0.0, 2.0)
        check_range("top_p", top_p, 0.0, 1.0)
        check_range("frequency_penalty", frequency_penalty, -2.0, 2.0)
        check_range("presence_penalty", presence_penalty, -2.0, 2.0)
        check_min("max_tokens", max_tokens, 1)
        check_min("max_completion_tokens", max_completion_tokens, 1)
        if reasoning_effort is not None and reasoning_effort not in _REASONING_EFFORTS:
            raise ValueError(
                f"reasoning_effort must be one of {sorted(_REASONING_EFFORTS)}"
            )
        # ``-1`` is the NIM-defined sentinel for "no budget enforcement";
        # any other negative value is invalid.
        if reasoning_budget is not None and reasoning_budget < -1:
            raise ValueError(
                "reasoning_budget must be -1 (disabled) or >= 0 "
                f"(got {reasoning_budget})"
            )

        self._model_name = model_name
        self._api_key = api_key
        self._base_url = base_url
        self._temperature = temperature
        # ``max_tokens`` and ``max_completion_tokens`` are equivalent on
        # the NIM API. ChatNVIDIA accepts ``max_completion_tokens`` as
        # input (and aliases it to its internal ``max_tokens`` field).
        # If the user sets both, prefer the UI-style ``max_tokens``.
        self._max_tokens = (
            max_tokens if max_tokens is not None else max_completion_tokens
        )
        self._top_p = top_p
        self._frequency_penalty = frequency_penalty
        self._presence_penalty = presence_penalty
        self._seed = seed
        self._stop = stop
        self._streaming = streaming
        self._reasoning_effort = reasoning_effort
        self._reasoning_budget = reasoning_budget
        self._chat_template_kwargs = chat_template_kwargs
        self._default_headers = default_headers
        self._extra_body = extra_body
        self._model_kwargs = model_kwargs

    def build(  # noqa: PLR0912 — orchestrates many independent kwargs.
        self, ctx: RunContext, config: dict[str, Any]
    ) -> BaseChatModel:
        try:
            from langchain_nvidia_ai_endpoints import ChatNVIDIA
        except ImportError as exc:  # pragma: no cover — install-hint path
            raise RuntimeError(
                "NvidiaProvider requires langchain-nvidia-ai-endpoints. "
                "Install the agent_server with the [nvidia] extra."
            ) from exc

        api_key = ctx.api_keys.get("NVIDIA_API_KEY") or self._api_key

        kwargs: dict[str, Any] = {
            "model": config.get("model_name", self._model_name),
            "temperature": self._temperature,
            # ChatNVIDIA inverts the streaming flag — its native field
            # is ``disable_streaming``.
            "disable_streaming": not self._streaming,
        }
        if api_key:
            kwargs["api_key"] = api_key
        if self._base_url is not None:
            kwargs["base_url"] = self._base_url
        if self._max_tokens is not None:
            # ChatNVIDIA deprecated ``max_tokens`` in favour of
            # ``max_completion_tokens``; both feed the same internal
            # field but only the new name dodges the deprecation
            # warning.
            kwargs["max_completion_tokens"] = self._max_tokens
        if self._top_p is not None:
            kwargs["top_p"] = self._top_p
        if self._seed is not None:
            kwargs["seed"] = self._seed
        if self._stop is not None:
            kwargs["stop"] = self._stop
        if self._default_headers is not None:
            kwargs["default_headers"] = dict(self._default_headers)

        # Non-native fields piggy-back on ``model_kwargs`` so they get
        # spread into the chat-completion payload as top-level body
        # fields. Passing them via ``extra_body`` triggers a langchain
        # warning ("I'm transferring this to model_kwargs for you, are
        # you sure?") — merge them ourselves so the warning never
        # fires.
        merged_model_kwargs: dict[str, Any] = {}
        if self._extra_body:
            merged_model_kwargs.update(self._extra_body)
        if self._frequency_penalty is not None:
            merged_model_kwargs["frequency_penalty"] = self._frequency_penalty
        if self._presence_penalty is not None:
            merged_model_kwargs["presence_penalty"] = self._presence_penalty
        if self._reasoning_effort is not None:
            merged_model_kwargs["reasoning_effort"] = self._reasoning_effort
        if self._reasoning_budget is not None:
            merged_model_kwargs["reasoning_budget"] = self._reasoning_budget
        if self._chat_template_kwargs is not None:
            # NIM accepts ``chat_template_kwargs`` as a top-level field;
            # legacy path for ``reasoning_budget``. The explicit
            # ``reasoning_budget`` field wins when both are set, but
            # we forward both so the backend can choose.
            merged_model_kwargs["chat_template_kwargs"] = dict(
                self._chat_template_kwargs
            )
        if self._model_kwargs:
            merged_model_kwargs.update(self._model_kwargs)
        if merged_model_kwargs:
            kwargs["model_kwargs"] = merged_model_kwargs
        model = ChatNVIDIA(**kwargs)
        _silence_unknown_tools_warning(model)
        return model
