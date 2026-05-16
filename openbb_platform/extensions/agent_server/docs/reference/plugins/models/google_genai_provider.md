# `openbb_agent_server.plugins.models.google_genai_provider`

Wraps `langchain_google_genai.ChatGoogleGenerativeAI` against Google's public Gemini API (`generativelanguage.googleapis.com`) — the API-key-authenticated path rather than the GCP-IAM-authenticated Vertex path. Same parameter surface as Vertex except for the lack of `project` / `location` / `credentials` and the addition of `api_key` / `base_url`. Exposes Gemini's thinking controls, structured-output fields, and context-caching `cached_content`. Optional install: `[google_genai]`.

**Source:** [`openbb_agent_server/plugins/models/google_genai_provider.py`](../../../../openbb_agent_server/plugins/models/google_genai_provider.py)

## Classes

### `GoogleGenAIProvider`

Plugin entry-point name: `google_genai`.

`build(ctx, config) -> BaseChatModel` lazy-imports `ChatGoogleGenerativeAI` (install hint on `ImportError`) and returns a freshly-constructed instance.

## Parameters

Keyword-only. Validation in `__init__` via [`_validation.py`](_validation.md): `check_range("temperature", …, 0.0, 2.0)`, `check_range("top_p", …, 0.0, 1.0)`, `check_min("top_k", …, 1)`, `check_min("max_output_tokens", …, 1)`, `check_min("max_retries", …, 0)`, `check_min("thinking_budget", …, 0)`. `thinking_level` ∈ `{"minimal", "low", "medium", "high"}`.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_name` | `str` | `"gemini-2.5-flash"` | Gemini model id. Overridable by `config["model_name"]`. |
| `api_key` | `str \| None` | `None` | Static fallback. See resolution below. Forwarded as `google_api_key`. |
| `temperature` | `float` | `0.0` | `[0, 2]`. |
| `max_output_tokens` | `int \| None` | `None` | Generated-token ceiling. `>= 1`. |
| `top_p` | `float \| None` | `None` | `[0, 1]`. |
| `top_k` | `int \| None` | `None` | `>= 1`. |
| `timeout` | `float \| None` | `None` | Per-request timeout. |
| `max_retries` | `int` | `6` | `>= 0`. Same higher default as Vertex — Google's public API also returns transient 503s during rollouts. |
| `seed` | `int \| None` | `None` | Best-effort determinism. |
| `stop` | `list[str] \| None` | `None` | Stop sequences. Copied via `list(...)`. |
| `safety_settings` | `dict[Any, Any] \| None` | `None` | Gemini safety filters (harm category → threshold). |
| `base_url` | `str \| None` | `None` | Custom Gemini API endpoint (rare; for proxies). |
| `additional_headers` | `dict[str, str] \| None` | `None` | Per-request headers. Copied before forwarding. |
| `cached_content` | `str \| None` | `None` | Resource name of a pre-uploaded cached-content entry (Gemini context-caching). |
| `response_mime_type` | `str \| None` | `None` | E.g. `"application/json"` to force JSON output. |
| `response_schema` | `dict[str, Any] \| None` | `None` | JSON schema for structured-output mode. |
| `thinking_budget` | `int \| None` | `None` | Max thinking tokens, `>= 0`. |
| `thinking_level` | `str \| None` | `None` | One of `{"minimal", "low", "medium", "high"}`. |
| `include_thoughts` | `bool \| None` | `None` | Surface the model's thinking trace on the response. |
| `labels` | `dict[str, str] \| None` | `None` | Optional labels forwarded to the API. Copied before forwarding. |

## API key resolution

Resolution order at `build` time:

1. `ctx.api_keys["GOOGLE_API_KEY"]`.
2. `ctx.api_keys["GEMINI_API_KEY"]` — alternate name the principal store may use.
3. `self._api_key`.

If a key resolves it is forwarded as `google_api_key=...`. If none resolves the kwarg is omitted and the SDK falls back to its own env-var lookup (`GOOGLE_API_KEY`).

## Tool choice and parallel calls

Set per-call by middleware. Gemini supports `tool_choice="auto"` / `"any"` / explicit function selection. **Gemini does not emit parallel tool calls** — at most one function call per turn, just like the Vertex path. Sequence the calls across turns.

## TOML example

```toml
[agent.model]
type = "google_genai"

[agent.model.config]
model_name = "gemini-2.5-flash"
temperature = 0.0
max_output_tokens = 4096
thinking_level = "low"
response_mime_type = "application/json"
additional_headers = { "X-Tenant" = "acme" }
```

## Notes

- Use this provider only for the API-key-authenticated Gemini endpoint. For Vertex (GCP-IAM-authenticated, project/region scoped, IAM-bound billing) use [`vertex_provider`](vertex_provider.md). The two share the same underlying langchain class but with `vertexai=False` (here) vs `vertexai=True` (there).
- `max_retries=6` matches Vertex — same model family, same transient-503 behaviour during rollouts.
- `cached_content` references a Gemini cached-content resource you've created out-of-band — this provider does not create or refresh caches.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
