# `openbb_agent_server.plugins.models.anthropic_provider`

Wraps `langchain_anthropic.ChatAnthropic` so Claude models can be selected as the agent's chat model via a profile. Supports the full Anthropic Messages API surface — extended `thinking`, beta header opt-ins (`betas`), custom `api_url` (for proxies / regional gateways), per-request `default_headers`, parallel tool calls (Anthropic's default), and the four tool-choice modes the Messages API exposes (`auto`, `any`, `tool`, `none`).

**Source:** [`openbb_agent_server/plugins/models/anthropic_provider.py`](../../../../openbb_agent_server/plugins/models/anthropic_provider.py)

## Classes

### `AnthropicProvider`

Plugin entry-point name: `anthropic`.

`build(ctx, config) -> BaseChatModel` returns a freshly-constructed `ChatAnthropic`. The runtime calls this once per agent run.

## Parameters

All parameters are keyword-only. Numeric constraints are enforced eagerly in `__init__` via [`_validation.py`](_validation.md) helpers — `check_range("temperature", …, 0.0, 1.0)`, `check_range("top_p", …, 0.0, 1.0)`, `check_min("top_k", …, 1)`, `check_min("max_tokens", …, 1)`, `check_min("max_retries", …, 0)`.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_name` | `str` | `"claude-opus-4-7"` | Anthropic model id. May be overridden per-run by `config["model_name"]`. |
| `api_key` | `str \| None` | `None` | Static fallback. See API-key resolution below. |
| `api_url` | `str \| None` | `None` | Custom base URL, mapped to `ChatAnthropic(anthropic_api_url=...)`. Use for regional gateways or proxies. |
| `max_tokens` | `int` | `8192` | Hard ceiling on generated tokens. `>= 1`. |
| `temperature` | `float` | `0.0` | Sampling temperature in `[0, 1]`. |
| `top_p` | `float \| None` | `None` | Nucleus sampling in `[0, 1]`. Omitted when `None`. |
| `top_k` | `int \| None` | `None` | Top-k sampling, `>= 1`. Omitted when `None`. |
| `stop_sequences` | `list[str] \| None` | `None` | Hard stop strings. Copied via `list(...)` before forwarding. |
| `timeout` | `float \| None` | `None` | Mapped to `ChatAnthropic(default_request_timeout=...)`. |
| `max_retries` | `int` | `2` | Transient-error retries. `>= 0`. |
| `default_headers` | `dict[str, str] \| None` | `None` | Attached to every request — handy for routing or tenant headers. |
| `thinking` | `dict[str, Any] \| None` | `None` | Extended thinking config (e.g. `{"type": "enabled", "budget_tokens": 16000}`). |
| `betas` | `list[str] \| None` | `None` | Anthropic beta header opt-ins, forwarded as `betas=[...]`. |
| `streaming` | `bool` | `True` | If True, partial deltas stream via SSE; the OpenBB agent server's SSE protocol assumes streaming. |

## API key resolution

Resolution order at `build` time:

1. `ctx.api_keys["ANTHROPIC_API_KEY"]` — populated from the request principal's API-key store (per-tenant credentials).
2. `self._api_key` — the static constructor kwarg from the profile TOML.

If neither yields a key, no `api_key` is passed to `ChatAnthropic`; the underlying client falls back to its own env-var lookup (`ANTHROPIC_API_KEY`).

## Tool choice and parallel calls

Anthropic's tool-choice modes (`auto`, `any`, `tool`, `none`) are not configured on the provider — they are emitted per `bind_tools` call by the agent middleware. `ChatAnthropic` runs parallel tool calls by default; nothing in this provider disables that.

## TOML example

```toml
[agent.model]
type = "anthropic"

[agent.model.config]
model_name = "claude-opus-4-7"
max_tokens = 16384
temperature = 0.0
max_retries = 3
default_headers = { "X-Tenant" = "acme" }
thinking = { type = "enabled", budget_tokens = 8000 }
betas = ["context-1m-2025-08-07"]
```

## Notes

- `model_name` is the only field the runtime can override per-run via `config["model_name"]`; every other field is fixed at profile-load time.
- `streaming=True` is the default and what the SSE-emitting runtime expects — disable only for non-streaming integrations.
- `betas=[...]` is the documented path for opting into Anthropic's preview features (e.g. 1M context, pdf input). Each beta header maps to a `anthropic-beta` value the SDK forwards.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
