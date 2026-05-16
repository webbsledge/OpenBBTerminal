# `openbb_agent_server.plugins.middleware.usage_recorder`

Captures `usage_metadata` from the last `AIMessage` after every model call and writes a row to the `usage` table via the history service. Backs per-user / per-trace token accounting; cost layering is intentionally left to an opt-in pricing plugin (`cost_usd` is always written as `0.0`).

**Source:** [`openbb_agent_server/plugins/middleware/usage_recorder.py`](../../../../openbb_agent_server/plugins/middleware/usage_recorder.py)

## Classes

### `UsageRecorderMiddlewareFactory`

Plugin entry-point name: `usage_recorder`. Enabled by default.

`build(ctx, config) -> AgentMiddleware` returns a per-run `_UsageRecorderMiddleware`. No config keys.

## Behaviour

- Hooks `aafter_model` — runs after the framework has appended the assistant turn to state. The hook returns `None` so state is untouched.
- Reads the last message off `state["messages"]`. If there are no messages or the last one has no `usage_metadata`, the hook is a no-op (skips streaming-only providers that don't fill the field).
- Looks up the current `RunContext` via `run_context.current()`. If no context is bound (e.g. the middleware is being exercised outside the FastAPI request flow) the hook logs at DEBUG and skips.
- Reads the model name from `response_metadata.model` / `model_name` / `ls_model_name`, falling back to `additional_kwargs.model` / `model_name`, and finally `"unknown"`. Different langchain providers stash the model name in different places — the priority order here matches the observed coverage across the NIM / Anthropic / OpenAI integrations.
- Builds a `UsageRecord` with `input_tokens`, `output_tokens`, `cache_read` and `cache_creation` taken from `usage_metadata` / `usage_metadata["input_token_details"]`. Anthropic-style cache fields are picked up automatically; providers that don't report them record `0`.
- Calls `services.get_history().record_usage(...)` keyed by `ctx.principal` and `ctx.trace_id`.

## TOML config example

```toml
[agent.middleware]
# Enabled by default; remove from the list to stop persisting token usage.

[agent.middleware_config.usage_recorder]
# (no knobs)
```

## `UsageRecord` fields written

| Field | Source | Notes |
| --- | --- | --- |
| `trace_id` | `ctx.trace_id` | The current run's trace id. |
| `user_id` | `ctx.principal.user_id` | Always present — the principal is set by the auth backend before the middleware runs. |
| `model` | `response_metadata.model` / `model_name` / `ls_model_name` → `additional_kwargs.model` / `model_name` → `"unknown"` | First non-empty wins; the priority order matches the observed coverage across NIM / Anthropic / OpenAI integrations. |
| `input_tokens` | `usage_metadata["input_tokens"]` | Cast to `int`. |
| `output_tokens` | `usage_metadata["output_tokens"]` | Cast to `int`. |
| `cache_read` | `usage_metadata["input_token_details"]["cache_read"]` | Anthropic-style cache reads; `0` for providers that don't report it. |
| `cache_creation` | `usage_metadata["input_token_details"]["cache_creation"]` | Anthropic-style cache creations; `0` for providers that don't report it. |
| `cost_usd` | `0.0` (hard-coded) | Cost layering is left to an opt-in pricing plugin so the recorder doesn't have to know provider tariffs. |

## See also

- [`../../runtime/services.md`](../../runtime/services.md) — `services.get_history().record_usage()`.
- [`../../persistence/store.md`](../../persistence/store.md) — `UsageRecord` schema.
- [`../../../operating/observability.md`](../../../operating/observability.md) — how the rows surface in the trace viewer.
- [`tool_call_ledger`](tool_call_ledger.md) — the tool-call analogue of this middleware.
