# `openbb_agent_server.plugins.middleware.tool_call_ledger`

Persists every tool call — args, result, error, latency — into the `tool_calls` table via the history service. Backs the post-hoc audit log used by `/agents/{id}/trace/{trace_id}` and the operating-mode observability views.

**Source:** [`openbb_agent_server/plugins/middleware/tool_call_ledger.py`](../../../../openbb_agent_server/plugins/middleware/tool_call_ledger.py)

## Classes

### `ToolCallLedgerMiddlewareFactory`

Plugin entry-point name: `tool_call_ledger`. Enabled by default.

`build(ctx, config) -> AgentMiddleware` returns a per-run `_ToolCallLedgerMiddleware`. No config keys.

## Behaviour

- `awrap_tool_call` is the only hook. The middleware looks up the current `RunContext` via `run_context.current()` — if no context is bound (e.g. the middleware is being exercised outside the FastAPI request flow) it silently falls through to the handler without recording anything.
- Tool name resolution mirrors `tool_call_announcer`: `request.tool_name` → `request.tool_call["name"]` → `"unknown"`. Args are pulled from `request.args` or `request.tool_call["args"]`.
- Wraps the handler in `time.perf_counter()` for latency. The recorded `latency_ms` is the wall time of the tool call, including any framework dispatch overhead.
- On success, records `state="complete"` with `result={"result": <safe_json(response.content or response)>}`.
- On exception (other than `GraphBubbleUp`, which is re-raised untouched), records `state="error"` with `error=str(exc)` and `result=None`, then re-raises so the agent loop can surface the failure.
- `_safe_json` round-trips the value through `json.dumps`; non-serialisable values fall back to `{"__str__": str(value)}` so the row write never fails on an exotic tool result.
- Every record is keyed by `principal=ctx.principal` and `trace_id=ctx.trace_id` and stamped with `side="server"` — distinguishing tools run inside the agent process from client-side tools resolved by the protocol adapter.

## TOML config example

```toml
[agent.middleware]
# Enabled by default; remove from the list to disable persistent tool-call logging.

[agent.middleware_config.tool_call_ledger]
# (no knobs)
```

## See also

- [`../../runtime/services.md`](../../runtime/services.md) — `services.get_history()` and the history-service contract.
- [`../../persistence/models.md`](../../persistence/models.md) — the `tool_calls` table schema.
- [`tool_call_announcer`](tool_call_announcer.md) — the UI twin.
- [`../../../operating/observability.md`](../../../operating/observability.md) — how the ledger feeds the trace viewer.
