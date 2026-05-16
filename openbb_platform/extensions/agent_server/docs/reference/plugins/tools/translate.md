# `openbb_agent_server.plugins.tools.translate`

Agent-callable text translation via NVIDIA's Riva translate model. One tool, one model, swap-via-config; markdown and code fences pass through unchanged.

**Source:** [`openbb_agent_server/plugins/tools/translate.py`](../../../../openbb_agent_server/plugins/tools/translate.py)

## Classes

### `NvidiaTranslateToolSource`

Plugin entry-point name: `translate`. Constructor takes `model`, `api_key`, `base_url`, `temperature`, `max_tokens`. `tools(ctx, config)` builds one `NvidiaTranslator` per agent run (api key sourced from `ctx.api_keys["NVIDIA_API_KEY"]` → config → constructor) and registers a single `StructuredTool`.

| Tool | Args | Returns |
| --- | --- | --- |
| `translate` | `text: str` (string to translate), `source_language: str = "auto"` (English-label, e.g. `"French"`, `"Mandarin"`, `"Spanish"`; `"auto"` lets the model detect), `target_language: str = "English"` (English-label of the target) | The translated string. On failure, returns `"translation failed: <error>"` rather than raising — the agent loop can still proceed. |

Markdown, code fences, and numeric tokens are preserved by the underlying `NvidiaTranslator` (see [`memory/translation.py`](../../runtime/index.md) for the wrapper).

### Failure handling

The tool catches any exception raised by `NvidiaTranslator.translate`, logs at WARNING level, and returns the string `"translation failed: <error>"`. The agent sees a normal tool result rather than a `ToolException`, so the agent loop continues — typically the model will try a different approach or surface the failure to the user.

This is intentional: translation is a "nice-to-have" surface, and a transient NIM hiccup should not derail an otherwise successful turn. Callers that need hard-fail semantics can inspect the return string for the `"translation failed:"` prefix.

## Config

`[agent.tool_source_config.translate]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `model` | string | `"nvidia/riva-translate-4b-instruct-v1_1"` | Any NIM-compatible translate model. |
| `api_key` | string | `None` | Forwarded to `NvidiaTranslator`; falls back to `ctx.api_keys["NVIDIA_API_KEY"]`. |
| `base_url` | string | `None` | Override the NIM endpoint. |
| `temperature` | float | `0.0` | Sampling temperature. |
| `max_tokens` | int | `2048` | Hard cap on the model's reply. |

## Related

- [`rerank` tool source](rerank.md) — sibling NIM specialist wrapper.
- [Operating: configuration](../../../operating/configuration.md) — forwarding `NVIDIA_API_KEY` via `QueryRequest.api_keys`.
