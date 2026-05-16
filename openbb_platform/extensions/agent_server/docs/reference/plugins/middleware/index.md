# `openbb_agent_server.plugins.middleware`

Built-in agent middleware. Each entry below is a separate plugin entry-point loaded by the runtime registry; all eight are listed in `AgentServerSettings.middleware` by default — drop one from the tuple in a profile to disable it.

- [`call_limit`](call_limit.md) — hard cap on model calls per run. Wraps langchain's `ModelCallLimitMiddleware`.
- [`loop_guard`](loop_guard.md) — short-circuits identical consecutive tool calls; returns a synthetic `ToolMessage` telling the model to stop and answer.
- [`tool_call_announcer`](tool_call_announcer.md) — emits a `copilotStatusUpdate` reasoning step for every tool call so the Workspace UI shows live progress.
- [`tool_call_ledger`](tool_call_ledger.md) — records each tool call (args, result, latency, error) into the `tool_calls` table via the history service.
- [`tool_call_limit`](call_limit.md) — hard cap on tool runs per run. Wraps langchain's `ToolCallLimitMiddleware`. Same source file as `call_limit`.
- [`tool_filter`](tool_filter.md) — drops tools by name before the model sees them. Default set targets DeepAgents' filesystem tools.
- [`tool_message_normaliser`](tool_message_normaliser.md) — rewrites tool messages into a strict, provider-tolerant shape before every model call. Handles the Mistral `role:"tool"` path and mirrors `tool_calls` into `additional_kwargs` for langchain-nvidia compat.
- [`usage_recorder`](usage_recorder.md) — captures `usage_metadata` from each `AIMessage` and writes a `usage` row.

**Source:** [`openbb_agent_server/plugins/middleware/__init__.py`](../../../../openbb_agent_server/plugins/middleware/__init__.py)

## See also

- [`../../runtime/plugins.md`](../../runtime/plugins.md) — the `Middleware` plugin protocol and the `build(ctx, config)` contract.
- [`../../../developing/writing-a-middleware.md`](../../../developing/writing-a-middleware.md) — authoring a new middleware.
- [`../../../operating/profiles.md`](../../../operating/profiles.md) — `[agent.middleware_config.*]` configuration.
