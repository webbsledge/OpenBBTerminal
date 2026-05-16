# `openbb_agent_server.plugins.models.openai_provider`

Wraps `langchain_openai.ChatOpenAI` against the real OpenAI API. Exposes the full Chat Completions surface — sampling (`temperature`, `top_p`, `presence_penalty`, `frequency_penalty`, `seed`, `n`), `reasoning_effort` for the `o*` reasoning models, organization headers, and custom `base_url` for OpenAI-tenant proxies (Azure-style routing). Optional install: `[openai]`.

**Source:** [`openbb_agent_server/plugins/models/openai_provider.py`](../../../../openbb_agent_server/plugins/models/openai_provider.py)

## Classes

### `OpenAIProvider`

Plugin entry-point name: `openai`.

`build(ctx, config) -> BaseChatModel` imports `langchain_openai.ChatOpenAI` lazily (raising a useful install hint if the optional extra is missing) and returns a freshly-constructed instance.

## Parameters

Keyword-only. Validation in `__init__` via [`_validation.py`](_validation.md):
`check_range("temperature", …, 0.0, 2.0)`, `check_range("top_p", …, 0.0, 1.0)`, `check_range("presence_penalty"/"frequency_penalty", …, -2.0, 2.0)`, `check_min("max_tokens", …, 1)`, `check_min("n", …, 1)`, `check_min("max_retries", …, 0)`. `reasoning_effort` is checked against the set `{"minimal", "low", "medium", "high"}`.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_name` | `str` | `"gpt-4o"` | OpenAI model id. Overridable per-run by `config["model_name"]`. |
| `api_key` | `str \| None` | `None` | Static fallback. See resolution below. |
| `base_url` | `str \| None` | `None` | Custom OpenAI-compatible URL. Use [`openai_compat_provider`](openai_compat_provider.md) for non-OpenAI backends. |
| `organization` | `str \| None` | `None` | Mapped to `openai_organization`. |
| `temperature` | `float` | `0.0` | `[0, 2]`. |
| `max_tokens` | `int \| None` | `None` | Generated-token ceiling. `>= 1`. Omitted when `None` — server picks. |
| `top_p` | `float \| None` | `None` | Nucleus sampling, `[0, 1]`. |
| `presence_penalty` | `float \| None` | `None` | `[-2, 2]`. |
| `frequency_penalty` | `float \| None` | `None` | `[-2, 2]`. |
| `seed` | `int \| None` | `None` | Best-effort determinism. |
| `n` | `int` | `1` | Completions per request, `>= 1`. Tool-using agents want `1`. |
| `stop` | `list[str] \| str \| None` | `None` | Stop sequences. |
| `timeout` | `float \| None` | `None` | Mapped to `request_timeout`. |
| `max_retries` | `int` | `2` | `>= 0`. |
| `reasoning_effort` | `str \| None` | `None` | `"minimal" \| "low" \| "medium" \| "high"` — for `o*` reasoning models. |
| `default_headers` | `dict[str, str] \| None` | `None` | Per-request headers. Copied before forwarding. |
| `streaming` | `bool` | `True` | SSE deltas. |

## API key resolution

1. `ctx.api_keys["OPENAI_API_KEY"]`.
2. `self._api_key`.

If neither resolves, `api_key` is omitted from the ChatOpenAI kwargs and the SDK falls back to its own env-var lookup (`OPENAI_API_KEY`).

## Tool choice and parallel calls

`tool_choice` is set per-call by the agent middleware, not on the provider. OpenAI accepts `"auto"`, `"required"`, `"none"`, or an explicit `{"type": "function", "function": {"name": "..."}}`. Parallel tool calling is on by default in `gpt-4*` / `o*` — disabled (per call) by passing `parallel_tool_calls=False` through `bind_tools`.

## TOML example

```toml
[agent.model]
type = "openai"

[agent.model.config]
model_name = "gpt-4o"
temperature = 0.0
max_tokens = 4096
presence_penalty = 0.0
seed = 42
reasoning_effort = "medium"
default_headers = { "X-Tenant" = "acme" }
```

## Notes

- `OPENAI_API_KEY` from the request principal takes precedence over the static profile kwarg, so multi-tenant deployments can keep per-tenant keys in their secret store and leave the profile config blank.
- `base_url` is supported but is intended for OpenAI's own first-party proxies / Azure-OpenAI compatibility. For self-hosted servers (vLLM, NIM, TGI, Ollama, LM Studio) use [`openai_compat_provider`](openai_compat_provider.md), which also accepts `reasoning_budget` and the local-server placeholder API key.
- `reasoning_effort` is only meaningful for the `o*` family; passing it to a non-reasoning model is harmless (the field is ignored on the wire).

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
