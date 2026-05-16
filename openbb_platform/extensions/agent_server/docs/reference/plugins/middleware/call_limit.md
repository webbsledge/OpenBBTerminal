# `openbb_agent_server.plugins.middleware.call_limit`

Hard caps on model calls and tool runs per agent run. When a cap is hit the agent exits cleanly with whatever final answer the model can produce instead of streaming forever — used as a backstop against runaway loops that slip past `loop_guard`.

The module ships **two** factory classes from the same file. Both are listed in `AgentServerSettings.middleware` and registered separately under their own entry-point names.

**Source:** [`openbb_agent_server/plugins/middleware/call_limit.py`](../../../../openbb_agent_server/plugins/middleware/call_limit.py)

## Classes

### `CallLimitMiddlewareFactory`

Plugin entry-point name: `call_limit`. Enabled by default. Wraps langchain's `ModelCallLimitMiddleware`.

`build(ctx, config) -> AgentMiddleware` returns a `ModelCallLimitMiddleware` configured from the factory defaults overridden by `config`.

| Config key | Type | Default | Effect |
| --- | --- | --- | --- |
| `model_run_limit` | `int \| None` | `40` | Maximum number of `model_call` invocations per run. 40 is enough headroom for multi-step PDF research (search → outline → several `pdf_extract` calls → final answer) without leaving the door open for runaway loops. |
| `exit_behavior` | `str` | `"end"` | Forwarded to langchain. `"end"` jumps to the final-answer node; other values follow langchain's `ModelCallLimitMiddleware` semantics. |

### `ToolCallLimitMiddlewareFactory`

Plugin entry-point name: `tool_call_limit`. Enabled by default. Wraps langchain's `ToolCallLimitMiddleware` for an overall tool-run cap (per-tool caps come from langchain's middleware directly).

`build(ctx, config) -> AgentMiddleware` returns a `ToolCallLimitMiddleware`.

| Config key | Type | Default | Effect |
| --- | --- | --- | --- |
| `tool_run_limit` | `int \| None` | `80` | Maximum number of tool invocations per run across all tools. |
| `exit_behavior` | `str` | `"end"` | Same semantics as the model-call factory above. |

## Behaviour

- Both middlewares delegate to langchain's built-in classes — no custom hooks. The factories exist to (a) thread per-profile config in via `[agent.middleware_config.*]` and (b) keep the entry-point names stable for the runtime registry.
- The two factories are deliberately separate entry-points (`call_limit` and `tool_call_limit`) even though they live in the same module: that way a profile can flip one off without disturbing the other.
- Hitting either cap triggers langchain's `exit_behavior` — with `"end"` the agent emits a final assistant turn from whatever messages have accumulated so far.

## TOML config example

```toml
[agent.middleware_config.call_limit]
model_run_limit = 60
exit_behavior = "end"

[agent.middleware_config.tool_call_limit]
tool_run_limit = 120
```

## See also

- [`loop_guard`](loop_guard.md) — the soft variant that short-circuits identical-args repeats before the hard cap fires.
- [`../../runtime/plugins.md`](../../runtime/plugins.md) — the `Middleware` plugin protocol.
- [`../../../operating/profiles.md`](../../../operating/profiles.md) — how `[agent.middleware_config.*]` flows into `build(ctx, config)`.
