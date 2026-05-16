# `openbb_agent_server.plugins.tools.client_side`

Declares **stub** tools that the Workspace UI executes — not the server. Each registered tool pauses the LangGraph run via `interrupt(...)` and hands control back to Workspace, which performs the action client-side and resumes the run by posting the result. The shared building block for the [`dashboard`](dashboard.md) tool source and any other client-only surface.

**Source:** [`openbb_agent_server/plugins/tools/client_side.py`](../../../../openbb_agent_server/plugins/tools/client_side.py)

## Classes

### `ClientSideToolSource`

Plugin entry-point name: `client_side`. Constructor takes `tools: list[dict]` — each spec is `{name, description?, parameters?}` in JSON-Schema shape. `tools(ctx, config)` reads `config.get("tools", self._specs)` and returns one `StructuredTool` per spec through `_make_tool`.

| Tool surface | Args | Returns |
| --- | --- | --- |
| **One tool per spec** | Whatever the spec's `parameters` JSON-Schema declares; missing required values raise validation errors. | Whatever Workspace POSTs back as the resumed tool result. The model sees this on the next agent step. |

Tool names are prefixed with `CLIENT_SIDE_TOOL_PREFIX` (from `protocol.adapter`) so the adapter can distinguish client-side calls from server-side ones when streaming SSE frames.

### Spec format

```python
{
    "name": "open_widget",
    "description": "Open / focus a specific widget on the user's dashboard.",
    "parameters": {
        "type": "object",
        "properties": {"widget_id": {"type": "string"}},
        "required": ["widget_id"],
    },
}
```

`_args_model_from_schema(name, parameters)` walks the `properties` dict and emits a pydantic model. Property types are converted via `_JSON_TO_PY`:

| JSON-Schema type | Python type |
| --- | --- |
| `string` | `str` |
| `integer` | `int` |
| `number` | `float` |
| `boolean` | `bool` |
| `array` | `list` |
| `object` | `dict` |
| (other) | `Any` |

Optional properties become `<type> \| None` with default `None`; required properties keep their non-Optional type. The `description` from the JSON-Schema becomes the pydantic `Field.description`.

### Interrupt payload

When the model invokes a client-side tool, `_client_side` calls `langgraph.types.interrupt(...)` with:

```python
{"type": "client_side_tool_call", "name": raw_name, "arguments": dict(kwargs)}
```

LangGraph pauses the run with this payload. Workspace consumes the interrupt, runs the action, and resumes the run by posting the tool result, which becomes the model's next `ToolMessage`.

## Security

- The model controls only the `arguments` payload — the tool name and shape are operator config.
- No server-side side effects: the server only emits an SSE interrupt frame.
- Argument validation runs through the auto-generated pydantic model before the interrupt, so malformed args never reach Workspace.

## Config

`[agent.tool_source_config.client_side]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `tools` | list of `{name, description, parameters}` | `()` | Replace to expose a different client-side surface. |

## Related

- [`dashboard` tool source](dashboard.md) — concrete client-side surface built on this primitive.
- [`workspace_mcp` tool source](workspace_mcp.md) — same interrupt mechanism, prefixed differently for the MCP path.
- [`protocol/adapter`](../../runtime/index.md) — `CLIENT_SIDE_TOOL_PREFIX` definition.
