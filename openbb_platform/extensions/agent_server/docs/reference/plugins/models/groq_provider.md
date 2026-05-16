# `openbb_agent_server.plugins.models.groq_provider`

Wraps `langchain_groq.ChatGroq` for Groq Cloud — Llama 3.x, Kimi, Qwen, the `groq/compound*` agents, GPT-OSS, and the Whisper STT models. Auto-attaches a per-`(api_key, model_name)` shared rate limiter from [`groq_rate_limiter`](groq_rate_limiter.md) so the multi-bucket RPM / RPD / TPM / TPD quotas Groq publishes are enforced client-side without bursting into 429s. Exposes Groq's `service_tier` ("on_demand" / "flex" / "auto"), `reasoning_effort`, `reasoning_format`, and a `record_rate_limit_table` hook for overriding the published limits at construction time. Optional install: `[groq]`.

**Source:** [`openbb_agent_server/plugins/models/groq_provider.py`](../../../../openbb_agent_server/plugins/models/groq_provider.py)

## Classes

### `GroqProvider`

Plugin entry-point name: `groq`.

`build(ctx, config) -> BaseChatModel` lazy-imports `ChatGroq` (install hint on `ImportError`), resolves a rate limiter (the constructor override if set, otherwise the process-shared `get_limiter(api_key, model_name)`), attaches its callback handler so token usage feeds the TPM / TPD buckets, and returns a freshly-constructed `ChatGroq` instance.

## Parameters

Keyword-only. Validation in `__init__` via [`_validation.py`](_validation.md): `check_range("temperature", …, 0.0, 2.0)`, `check_range("top_p", …, 0.0, 1.0)`, `check_min("max_tokens", …, 1)`, `check_min("n", …, 1)`, `check_min("max_retries", …, 0)`. Choice-set checks for `reasoning_effort` ∈ `{"none", "low", "medium", "high", "default"}`, `reasoning_format` ∈ `{"parsed", "raw", "hidden"}`, `service_tier` ∈ `{"on_demand", "flex", "auto"}`.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_name` | `str` | `"llama-3.3-70b-versatile"` | Groq model id. Overridable by `config["model_name"]`. |
| `api_key` | `str \| None` | `None` | Static fallback. See resolution below. |
| `base_url` | `str \| None` | `None` | Custom Groq endpoint (rare). |
| `temperature` | `float` | `0.0` | `[0, 2]`. |
| `max_tokens` | `int \| None` | `None` | Generated-token ceiling. `>= 1`. |
| `top_p` | `float \| None` | `None` | `[0, 1]`. Routed via `model_kwargs` (Groq's wire schema). |
| `n` | `int` | `1` | Completions per request, `>= 1`. |
| `stop` | `list[str] \| str \| None` | `None` | Stop sequences. |
| `timeout` | `float \| None` | `None` | Forwarded as `request_timeout`. |
| `max_retries` | `int` | `5` | `>= 0`. Higher default than other providers to ride out 429s the limiter has not yet smoothed. |
| `reasoning_effort` | `str \| None` | `None` | One of `{"none", "low", "medium", "high", "default"}` — for reasoning models. |
| `reasoning_format` | `Literal["parsed", "raw", "hidden"] \| None` | `None` | How Groq surfaces the model's reasoning trace. |
| `service_tier` | `Literal["on_demand", "flex", "auto"]` | `"on_demand"` | Groq billing/queueing tier. |
| `default_headers` | `dict[str, str] \| None` | `None` | Per-request headers. Copied before forwarding. |
| `streaming` | `bool` | `True` | SSE deltas. |
| `rate_limit` | `BaseRateLimiter \| None` | `None` | Explicit limiter override. When `None`, the build path attaches the process-shared `get_limiter(api_key, model_name)`. If you pass a non-`GroqRateLimiter` here, no usage callback is attached. |
| `record_rate_limit_table` | `dict[str, GroqLimits \| dict[str, int \| None]] \| None` | `None` | Patches `GROQ_LIMITS` at construction time — register a new model or override published quotas. Values may be `GroqLimits` instances or kwargs dicts. |

## API key resolution

1. `ctx.api_keys["GROQ_API_KEY"]`.
2. `self._api_key`.

The resolved key (or empty string when none resolves) is the cache key for `get_limiter` — same key + same model name = same shared limiter across runs.

## Tool choice and parallel calls

Set per-call by middleware. Groq supports OpenAI-style `tool_choice` (`auto`, `required`, `none`, explicit function). Parallel tool calls work on Llama 3.3 70B and the `compound*` models; smaller models emit one tool call per turn.

## TOML example

Llama 3.3 70B with explicit retries and flex tier:

```toml
[agent.model]
type = "groq"

[agent.model.config]
model_name = "llama-3.3-70b-versatile"
temperature = 0.0
max_tokens = 8192
service_tier = "flex"
max_retries = 5
```

Override the published limits for a self-hosted Groq deployment with higher quotas:

```toml
[agent.model]
type = "groq"

[agent.model.config]
model_name = "llama-3.3-70b-versatile"
base_url = "https://groq.internal.example.com/openai/v1"

[agent.model.config.record_rate_limit_table.llama-3-3-70b-versatile]
rpm = 600
tpm = 200_000
```

## Notes

- The rate limiter is **shared across runs** keyed by `(api_key, model_name)` — every concurrent agent run hitting the same model on the same key contends on the same RPM / TPM buckets. See [`groq_rate_limiter`](groq_rate_limiter.md) for the bucket model.
- `top_p` is routed through `model_kwargs={"top_p": ...}` rather than the top-level `top_p` kwarg because that's where Groq's wire schema expects it.
- `record_rate_limit_table` mutates the module-level `GROQ_LIMITS` dict; the change persists for the life of the process and affects every subsequently-constructed provider for the same model. Construct providers in deterministic order if you rely on overrides.
- If you pass a custom `rate_limit` that is not a `GroqRateLimiter`, the build path skips attaching the usage callback — your custom limiter won't see token counts and only the RPM bucket will throttle.

See also: [`groq_rate_limiter`](groq_rate_limiter.md), [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
