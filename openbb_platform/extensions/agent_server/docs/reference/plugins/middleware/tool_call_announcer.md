# `openbb_agent_server.plugins.middleware.tool_call_announcer`

Surfaces every tool call to the user as a `copilotStatusUpdate` reasoning step. The Workspace UI renders these as live status rows so the user sees what the agent is doing as it does it; on `tool_call` errors a second `ERROR` step is emitted before the exception propagates.

**Source:** [`openbb_agent_server/plugins/middleware/tool_call_announcer.py`](../../../../openbb_agent_server/plugins/middleware/tool_call_announcer.py)

## Classes

### `ToolCallAnnouncerMiddlewareFactory`

Plugin entry-point name: `tool_call_announcer`. Enabled by default.

`build(ctx, config) -> AgentMiddleware` returns a per-run `_ToolCallAnnouncerMiddleware`. No config keys — the announcer is on-or-off via `AgentServerSettings.middleware`.

## Behaviour

- `awrap_tool_call` runs before the tool handler. It extracts the tool name from `request.tool_name` or `request.tool_call["name"]`, flattens `request.tool_call["args"]` into a string-keyed dict, and emits one `INFO` reasoning step titled `"Calling tool: <name>"` with the args as detail rows.
- Long arg values are clipped to `_MAX_ARG_VALUE_LEN = 400` chars with a trailing ellipsis. Workspace's status-detail panel renders dict entries as plain key/value rows of strings; oversized blobs (page text, embeddings, etc.) blow out the panel.
- Non-string args are JSON-encoded via `json.dumps(..., default=str)`; `None` becomes `""`.
- `ToolCall` shape detection handles both the LangChain v1 dict form and the attribute form (`getattr(tc, "name", None)`).
- On exception the announcer emits a second reasoning step with `event_type="ERROR"`, the exception message in the title, and `tool_name` in detail — then re-raises. `GraphBubbleUp` is re-raised without an extra step so checkpoint-bubble flows don't read as errors in the UI.
- Detail dict always starts with `tool_name` so the panel row order is deterministic.

## TOML config example

```toml
[agent.middleware]
# Listed by default — no extra config required.
# Omit it from this list if you want the agent to call tools silently.

[agent.middleware_config.tool_call_announcer]
# (intentionally empty — no knobs)
```

## Helpers

- `_stringify_arg(value)` — stringifies one arg value. `None → ""`, `str` passes through, scalars use `str()`, everything else goes through `json.dumps(value, ensure_ascii=False, default=str)` with a `TypeError`/`ValueError` fallback to `str(value)`, then clips to `_MAX_ARG_VALUE_LEN`.
- `_args_as_detail(args)` — flattens the args dict to a string-only mapping for the UI; non-dict / empty args return `{}`.
- `_tool_name(request)` — `request.tool_name` → `tool_call["name"]` → `"unknown"`, always coerced to `str`.

## See also

- [`tool_call_ledger`](tool_call_ledger.md) — the persistence twin; the announcer is the UI surface, the ledger is the durable record.
- [`../../runtime/emit.md`](../../runtime/emit.md) — `emit.reasoning_step` and the SSE event schema.
- [`../../protocol/sse.md`](../../protocol/sse.md) — the `copilotStatusUpdate` event over the wire.
