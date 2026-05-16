# `openbb_agent_server.plugins.models.vertex_provider`

Wraps `langchain_google_genai.ChatGoogleGenerativeAI` in Vertex mode (`vertexai=True`). For Google's Gemini family hosted through Vertex AI rather than the public `generativelanguage.googleapis.com` endpoint. Authenticates via GCP Application Default Credentials, not an API key. Exposes Gemini's thinking / reasoning controls (`thinking_budget`, `thinking_level`, `include_thoughts`), structured-output fields (`response_mime_type`, `response_schema`), `cached_content`, and GCP-side `labels` for billing attribution. Optional install: `[vertex]`.

**Source:** [`openbb_agent_server/plugins/models/vertex_provider.py`](../../../../openbb_agent_server/plugins/models/vertex_provider.py)

## Classes

### `VertexProvider`

Plugin entry-point name: `vertex`.

`build(ctx, config) -> BaseChatModel` lazy-imports `ChatGoogleGenerativeAI` (install hint on `ImportError`) and returns an instance constructed with `vertexai=True`.

## Parameters

Keyword-only. Validation in `__init__` via [`_validation.py`](_validation.md): `check_range("temperature", …, 0.0, 2.0)`, `check_range("top_p", …, 0.0, 1.0)`, `check_min("top_k", …, 1)`, `check_min("max_output_tokens", …, 1)`, `check_min("max_retries", …, 0)`, `check_min("thinking_budget", …, 0)`. `thinking_level` ∈ `{"minimal", "low", "medium", "high"}`.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_name` | `str` | `"gemini-2.0-flash-001"` | Gemini model id. Overridable by `config["model_name"]`. |
| `project` | `str \| None` | `None` | GCP project id. Falls back to ADC default project. |
| `location` | `str` | `"us-central1"` | GCP region. |
| `temperature` | `float` | `0.0` | `[0, 2]`. |
| `max_output_tokens` | `int \| None` | `None` | Generated-token ceiling. `>= 1`. |
| `top_p` | `float \| None` | `None` | `[0, 1]`. |
| `top_k` | `int \| None` | `None` | `>= 1`. |
| `seed` | `int \| None` | `None` | Best-effort determinism. |
| `stop` | `list[str] \| None` | `None` | Stop sequences. Copied via `list(...)`. |
| `max_retries` | `int` | `6` | `>= 0`. Higher default than other providers because Vertex returns transient 503s during model rollouts. |
| `timeout` | `float \| None` | `None` | Per-request timeout. |
| `safety_settings` | `dict[Any, Any] \| None` | `None` | Gemini safety filters (harm category → threshold). |
| `response_mime_type` | `str \| None` | `None` | E.g. `"application/json"` to force JSON output. |
| `response_schema` | `dict[str, Any] \| None` | `None` | JSON schema for structured-output mode. |
| `thinking_budget` | `int \| None` | `None` | Max thinking tokens, `>= 0`. |
| `thinking_level` | `str \| None` | `None` | One of `{"minimal", "low", "medium", "high"}`. |
| `include_thoughts` | `bool \| None` | `None` | Surface the model's thinking trace on the response. |
| `cached_content` | `str \| None` | `None` | Resource name of a pre-uploaded cached-content entry (Vertex context-caching). |
| `labels` | `dict[str, str] \| None` | `None` | GCP resource labels — useful for billing attribution. Copied before forwarding. |
| `additional_headers` | `dict[str, str] \| None` | `None` | Per-request headers. Copied before forwarding. |
| `credentials` | `Any` | `None` | Explicit `google.auth.credentials.Credentials` instance — bypass ADC. |

## API key resolution

Vertex does not use an API key. Authentication is through GCP Application Default Credentials: `credentials` kwarg → `GOOGLE_APPLICATION_CREDENTIALS` service-account JSON → `gcloud auth application-default login` → metadata server (GCE / GKE / Cloud Run workload identity). `ctx.api_keys` is not consulted.

## Tool choice and parallel calls

Set per-call by middleware. Gemini supports `tool_choice="auto"`, `"any"`, and explicit function selection. **Gemini does not emit parallel tool calls** — the model returns at most one function call per turn, even when multiple are available. Sequence the calls across turns rather than expecting fan-out.

## TOML example

Gemini 2.0 Flash on Vertex with structured output:

```toml
[agent.model]
type = "vertex"

[agent.model.config]
model_name = "gemini-2.0-flash-001"
project = "openbb-prod"
location = "us-central1"
temperature = 0.0
max_output_tokens = 8192
response_mime_type = "application/json"
labels = { team = "research", cost_center = "ai" }
```

Gemini 2.5 thinking model with explicit budget:

```toml
[agent.model]
type = "vertex"

[agent.model.config]
model_name = "gemini-2.5-pro"
project = "openbb-prod"
thinking_level = "high"
thinking_budget = 16000
include_thoughts = true
```

## Notes

- `vertexai=True` is hardcoded — this provider is *only* the Vertex path. For the public Gemini API (`generativelanguage.googleapis.com`) use [`google_genai_provider`](google_genai_provider.md).
- `max_retries=6` is the Vertex-tuned default; Vertex rolls model versions through 503s and the higher retry count prevents spurious agent failures during rollout windows.
- `response_schema` + `response_mime_type="application/json"` together enable Gemini's structured-output mode; one without the other is ignored.
- `cached_content` references a Vertex cached-content resource you've created out-of-band — the provider does not create or refresh the cache.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
