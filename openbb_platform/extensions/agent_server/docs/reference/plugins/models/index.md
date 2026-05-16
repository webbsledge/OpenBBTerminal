# `openbb_agent_server.plugins.models`

Built-in model providers — concrete `ModelProvider` subclasses the runtime registry resolves when a profile's `[agent.model].type` field names one of them. Each provider wraps a LangChain chat model class, performs eager parameter validation in `__init__` (via [`_validation`](_validation.md)), and defers the actual `BaseChatModel` construction to `build(ctx, config)` so per-run `RunContext` (principal API keys, run id) can feed into the wire-level credentials.

**Source:** [`openbb_agent_server/plugins/models/__init__.py`](../../../../openbb_agent_server/plugins/models/__init__.py)

## Providers

- [`anthropic_provider`](anthropic_provider.md) — Anthropic Claude via `langchain_anthropic.ChatAnthropic`. Supports extended thinking, beta header opt-ins, custom `api_url`, and parallel tool calls.
- [`openai_provider`](openai_provider.md) — Real OpenAI API via `langchain_openai.ChatOpenAI`. Includes `reasoning_effort` for the `o*` family and organization headers. Optional install: `[openai]`.
- [`openai_compat_provider`](openai_compat_provider.md) — Any OpenAI-compatible endpoint (vLLM, NIM, TGI, Ollama, LM Studio) via `langchain_openai.ChatOpenAI`. Adds `reasoning_budget`, `chat_template_kwargs`, and an `extra_body` / `model_kwargs` escape hatch; `base_url` required. Optional install: `[openai]`.
- [`nvidia_provider`](nvidia_provider.md) — NVIDIA NIM / NeMo / Foundation-Models via `langchain_nvidia_ai_endpoints.ChatNVIDIA`. Exposes the full NIM parameter surface, suppresses the noisy `supports_tools` allow-list warning, and routes non-native fields through `model_kwargs`. Optional install: `[nvidia]`.
- [`bedrock_provider`](bedrock_provider.md) — AWS Bedrock via `langchain_aws.ChatBedrock`. AWS-credential-chain auth (no API key), `top_p` / `top_k` / `stop_sequences` routed through per-family `model_kwargs`, Guardrails and Converse API support. Optional install: `[bedrock]`.
- [`vertex_provider`](vertex_provider.md) — Google Gemini on Vertex AI via `langchain_google_genai.ChatGoogleGenerativeAI` with `vertexai=True`. ADC auth, thinking / structured-output / context-caching controls, GCP billing labels. No parallel tool calls. Optional install: `[vertex]`.
- [`google_genai_provider`](google_genai_provider.md) — Public Gemini API (`generativelanguage.googleapis.com`) via the same LangChain class but with `vertexai=False`. API-key auth from `GOOGLE_API_KEY` / `GEMINI_API_KEY`. Same parameter surface as Vertex minus the GCP-specific fields. Optional install: `[google_genai]`.
- [`groq_provider`](groq_provider.md) — Groq Cloud via `langchain_groq.ChatGroq`. Auto-attaches a process-shared multi-bucket rate limiter from [`groq_rate_limiter`](groq_rate_limiter.md) keyed on `(api_key, model_name)`. Optional install: `[groq]`.
- [`groq_rate_limiter`](groq_rate_limiter.md) — RPM / RPD / TPM / TPD plus audio-seconds buckets for Groq's published quotas. Used by `groq_provider`; documented separately because the bucket model, snapshot API, and `record_rate_limit_table` override are useful in their own right.
- [`snowflake_provider`](snowflake_provider.md) — Snowflake Cortex Complete via `langchain_community.chat_models.ChatSnowflakeCortex`. In-account compliance path; account / user / password / role / warehouse / database / schema all resolve from `ctx.api_keys`. **No tool-calling support.**
- [`fake_provider`](fake_provider.md) — Deterministic in-process model for tests and demos. Accepts a scripted list of responses (plain strings or `AIMessage` with `tool_calls`) and streams them as `AIMessageChunk` / `tool_call_chunks` so the SSE pipeline sees realistic shapes. Always installed.
- [`_validation`](_validation.md) — `check_range` / `check_min` helpers used by every provider's `__init__` for numeric-bounds validation.

## Selecting a provider

The `type` field under `[agent.model]` in a profile TOML names the provider's plugin entry-point. Examples: `type = "anthropic"`, `type = "openai_compat"`, `type = "groq"`. The runtime's plugin registry resolves the name to the class, instantiates it once at profile-load time with `[agent.model.config]` as `__init__` kwargs, and then calls `build(ctx, config)` per run.

The `[agent.model.config]` table is also threaded into `build` as the `config` dict; `model_name` is the one field every provider honours as a per-run override (`config.get("model_name", self._model_name)`).

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
