# `openbb_agent_server.runtime.builder`

Compose every plugin axis (model + tools + sub-agents + middleware + checkpointer) into one DeepAgents agent and stream its events out as OpenBB SSE. The router calls this once per `/v1/query`.

**Source:** [`openbb_agent_server/runtime/builder.py`](../../../openbb_agent_server/runtime/builder.py)

## Entry point

### `async def run_agent(*, ctx, body, settings, profile=None) -> AsyncIterator[SSEEvent]`

| Arg | Type | Purpose |
| --- | --- | --- |
| `ctx` | `RunContext` | Bound run context — identity, widgets, files, api keys, trace ids. |
| `body` | `QueryRequest` | Raw wire payload. The builder consumes `messages` / `tools` / `workspace_state`. |
| `settings` | `AgentServerSettings` | App-level settings (consulted for default profile, tool/middleware lists, system-prompt path). |
| `profile` | `AgentProfile \| None` | Pre-resolved profile. When `None`, `settings.resolve_profile(ctx.agent_name)` is called. |

Yields one [`SSEEvent`](../protocol/schemas.md#union) per significant DeepAgents step — text chunks, reasoning rows, tool dispatches, artifacts, and the end-of-run citation batch. The router converts each into wire bytes via [`protocol/sse.md`](../protocol/sse.md).

## Construction order

1. **Resolve plugins.** The `ModelProvider`, every `ToolSource`, every `SubAgentSpec`, every `Middleware`, and the `CheckpointerProvider` are loaded by name from their entry-point groups via `runtime.registry`.
2. **Model.** `model_provider.build(ctx, {})` returns the `BaseChatModel` for this run.
3. **Tools.** `_resolve_tools(ctx, profile)` calls each `ToolSource.tools(ctx, config)`, concatenates results, dedupes by name, and records which tools are client-side (`name.startswith("client:")`) so the adapter knows to route their dispatches as `copilotFunctionCall` events.
4. **Sub-agents.** `_resolve_subagents` turns each `SubAgentSpec` into the dict shape DeepAgents wants (filtering `spec.tools` against the main tool list — names not present are dropped silently).
5. **Middleware.** Each `Middleware.build(ctx, config)` is called fresh per run so middleware state is per-run, not per-process.
6. **Widget pre-fetch optimisation.** `widget_store.list_entries(...)` populates `in_store` (set of widget UUIDs already cached locally). The system prompt's widget snapshot marks each entry with `data_in_store=true` so the agent reads via `read_widget_data` instead of round-tripping through `get_widget_data` again.
7. **System prompt.** `_build_system_prompt` substitutes `{timezone}`, `{today}`, `{widget_snapshot}`, `{file_snapshot}` into the prompt template; the per-turn addendum lists what's already ingested and what SQL surface is available.
8. **Agent.** `deepagents.create_deep_agent(model, tools, system_prompt, subagents, middleware)` plus the bound checkpointer produce the LangGraph state machine.
9. **Stream.** `agent.astream(stream_mode=["updates","messages","custom"], subgraphs=True)` yields raw events; [`DeepAgentEventAdapter`](../protocol/adapter.md) translates each into a typed `SSEEvent`.

## Cooperative cancellation

`run_agent` checks the `(user_id, run_id)`-keyed `asyncio.Event` registered by the router on every iteration. When the event fires (client disconnect or `POST /v1/conversations/{id}/cancel`), the generator raises `CancelledError` cleanly so the FastAPI handler can close the SSE response and the trace row gets `status="cancelled"`.

## Persistence side effects

The middleware list is the persistence pipeline:

- `usage_recorder` reads `usage_metadata` off every `AIMessage` and writes `UsageRecord` rows.
- `tool_call_ledger` records every server-side tool call (args, result, latency, side, state) into `tool_calls`.
- `memory_writer` (optional, gated by `memory:write` scope) extracts durable facts post-turn into `memories`.
