# `openbb_agent_server.plugins.models.openai_compat_provider`

Wraps `langchain_openai.ChatOpenAI` against an arbitrary OpenAI-compatible endpoint — vLLM, NVIDIA NIM, HuggingFace TGI, Ollama, LM Studio, llama.cpp server, anything that speaks the Chat Completions wire protocol. Exposes the full standard parameter surface plus the `reasoning_effort` / `reasoning_budget` / `chat_template_kwargs` fields that NIM-class servers expose for reasoning models, and an `extra_body` / `model_kwargs` escape hatch for non-standard knobs. Optional install: `[openai]`.

**Source:** [`openbb_agent_server/plugins/models/openai_compat_provider.py`](../../../../openbb_agent_server/plugins/models/openai_compat_provider.py)

## Classes

### `OpenAICompatProvider`

Plugin entry-point name: `openai_compat`.

`build(ctx, config) -> BaseChatModel` lazy-imports `ChatOpenAI` (with an install hint on `ImportError`) and returns a freshly-constructed instance pointed at `base_url`.

## Parameters

Keyword-only. `base_url` and `model_name` are required and raise `ValueError` if empty. Numeric validation via [`_validation.py`](_validation.md): `check_range("temperature", …, 0.0, 2.0)`, `check_range("top_p", …, 0.0, 1.0)`, `check_range("presence_penalty"/"frequency_penalty", …, -2.0, 2.0)`, `check_min("max_tokens", …, 1)`, `check_min("n", …, 1)`, `check_min("max_retries", …, 0)`. `reasoning_effort` ∈ `{"none", "minimal", "low", "medium", "high"}`. `reasoning_budget` must be `-1` (disabled) or `>= 0`.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `base_url` | `str` | **required** | OpenAI-compatible `/v1` root. No fallback. |
| `model_name` | `str` | **required** | Exact name the backend serves. Overridable by `config["model_name"]`. |
| `api_key` | `str \| None` | `None` | Static fallback. Local servers accept any placeholder; the build path falls through to `"EMPTY"` when no key is found. |
| `organization` | `str \| None` | `None` | Mapped to `openai_organization`. |
| `temperature` | `float` | `0.0` | `[0, 2]`. |
| `max_tokens` | `int \| None` | `None` | `>= 1`. |
| `top_p` | `float \| None` | `None` | `[0, 1]`. |
| `presence_penalty` | `float \| None` | `None` | `[-2, 2]`. |
| `frequency_penalty` | `float \| None` | `None` | `[-2, 2]`. |
| `seed` | `int \| None` | `None` | Best-effort determinism. |
| `n` | `int` | `1` | `>= 1`. |
| `stop` | `list[str] \| str \| None` | `None` | Stop sequences. |
| `timeout` | `float \| None` | `None` | Mapped to `request_timeout`. |
| `max_retries` | `int` | `2` | `>= 0`. |
| `reasoning_effort` | `str \| None` | `None` | One of `{"none", "minimal", "low", "medium", "high"}`. Native ChatOpenAI field on recent versions; harmless when the backend ignores it. |
| `reasoning_budget` | `int \| None` | `None` | NIM / vLLM thinking-token cap. `-1` disables enforcement; `>= 0` is a hard cap. Forwarded via `model_kwargs`. |
| `chat_template_kwargs` | `dict[str, Any] \| None` | `None` | Top-level field vLLM / NIM forward into the chat-template render — legacy path for `reasoning_budget`. Forwarded via `model_kwargs`. |
| `default_headers` | `dict[str, str] \| None` | `None` | Per-request headers. |
| `default_query` | `dict[str, Any] \| None` | `None` | Per-request query-string params (e.g. API versioning). |
| `streaming` | `bool` | `True` | SSE deltas. |
| `extra_body` | `dict[str, Any] \| None` | `None` | Body-field passthrough merged into `model_kwargs`. |
| `model_kwargs` | `dict[str, Any] \| None` | `None` | Body-field passthrough merged into `model_kwargs`. Wins on collision with `extra_body`. |

## API key resolution

1. `ctx.api_keys["OPENAI_COMPAT_API_KEY"]` — provider-specific override.
2. `ctx.api_keys["OPENAI_API_KEY"]` — fall back to the generic OpenAI key.
3. `self._api_key`.
4. The placeholder `"EMPTY"` — local servers (Ollama, LM Studio, llama.cpp) accept anything.

The key is always passed to `ChatOpenAI` so the SDK does not attempt its own env-var fallback at the wire level.

## Tool choice and parallel calls

`tool_choice` is set per-call by middleware. Most compat backends honour the OpenAI `auto` / `required` / `none` / explicit-function modes; some (older vLLM builds) only support `auto`. Parallel tool calls depend on the backend — vLLM and NIM emit them; TGI's tool support is single-call only.

## TOML example

vLLM server hosting Llama 3.3 70B:

```toml
[agent.model]
type = "openai_compat"

[agent.model.config]
base_url = "http://vllm:8000/v1"
model_name = "meta-llama/Llama-3.3-70B-Instruct"
temperature = 0.0
max_tokens = 4096
default_headers = { "X-Routing-Pool" = "gpu-a100" }
```

NIM reasoning model with explicit thinking budget:

```toml
[agent.model]
type = "openai_compat"

[agent.model.config]
base_url = "https://integrate.api.nvidia.com/v1"
model_name = "deepseek-ai/deepseek-r1"
reasoning_effort = "high"
reasoning_budget = 16000
```

## Notes

- The `extra_body` / `model_kwargs` / `chat_template_kwargs` paths all merge into `ChatOpenAI(model_kwargs=...)`; on key collision `model_kwargs` wins, then `chat_template_kwargs`, then `extra_body`. The merged dict is only attached when non-empty.
- `reasoning_budget` is the documented NIM / vLLM extension; backends that don't recognise it ignore the field. Forwarding `chat_template_kwargs` in parallel lets older NIM builds (which only read the budget from there) work without a separate code path.
- If you need a strictly first-party OpenAI client (org routing, native Files API), prefer [`openai_provider`](openai_provider.md) — this one is for self-hosted and partner endpoints.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
