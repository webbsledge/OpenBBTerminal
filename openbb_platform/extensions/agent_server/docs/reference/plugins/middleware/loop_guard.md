# `openbb_agent_server.plugins.middleware.loop_guard`

Short-circuits identical consecutive tool calls — when the model invokes the same `(tool_name, args)` pair more than `max_repeats` times in a row, the middleware returns a synthetic `ToolMessage` instructing the model to stop and answer.

**Source:** [`openbb_agent_server/plugins/middleware/loop_guard.py`](../../../../openbb_agent_server/plugins/middleware/loop_guard.py)

## Classes

### `LoopGuardMiddlewareFactory`

Plugin entry-point name: `loop_guard`. Enabled by default.

`build(ctx, config) -> AgentMiddleware` returns a per-run `_LoopGuardMiddleware` with the configured threshold.

| Config key | Type | Default | Effect |
| --- | --- | --- | --- |
| `max_repeats` | `int` | `2` | Number of consecutive identical `(name, args_hash)` invocations allowed before the guard trips. The first call is legitimate, the second is a plausible retry, the third+ short-circuits. |

## Behaviour

- `awrap_tool_call` hashes the request as `(tool_name, sha256(args)[:12])`. A change in either resets the counter and the trip state.
- On the first short-circuit, emits one `WARNING` reasoning step (`Loop guard: 'X' called N times with identical arguments — halting further calls and forcing the model to answer with what it has.`) and logs at WARNING level.
- Returned `ToolMessage` uses the default `success` status (NOT `error`) so the model reads the message as an unusual tool result rather than a hard failure that justifies giving up entirely.
- Subsequent identical calls keep returning the same synthetic `ToolMessage` silently until the model breaks the loop by changing tool or args.

## TOML config example

```toml
[agent.tool_source_config.loop_guard]  # passed through `build(ctx, config)`
max_repeats = 3
```
