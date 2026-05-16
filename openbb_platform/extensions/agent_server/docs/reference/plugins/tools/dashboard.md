# `openbb_agent_server.plugins.tools.dashboard`

Client-side widget / dashboard interaction tools. Each tool is a stub: when the model calls it, the run pauses via `interrupt(...)` and Workspace runs the actual UI action (open a widget, switch dashboards, add a new widget). The server never executes the side effect — it only declares the function signature so the model can target it.

**Source:** [`openbb_agent_server/plugins/tools/dashboard.py`](../../../../openbb_agent_server/plugins/tools/dashboard.py)

## Classes

### `DashboardToolSource`

Plugin entry-point name: `dashboard`. `tools(ctx, config)` returns one `StructuredTool` per spec in `_DEFAULT_TOOLS` (overridable via the constructor's `tools=` kwarg or `[agent.tool_source_config.dashboard].tools`). Each spec passes through [`client_side._make_tool`](client_side.md) — the resulting tools are prefixed with `CLIENT_SIDE_TOOL_PREFIX` and pause the run with a `client_side_tool_call` interrupt when invoked.

| Tool | Args | Returns |
| --- | --- | --- |
| `open_widget` | `widget_id: str` (uuid of the widget to focus) | Whatever Workspace POSTs back as the resumed tool result. |
| `highlight_widget` | `widget_id: str`, `duration_ms: int` (optional) | Workspace-side ack. |
| `change_dashboard` | `dashboard_id: str` | Workspace-side ack. |
| `add_widget_to_dashboard` | `widget_type: str` (template id), `params: object` (optional template params) | Workspace-side ack. |

The arg types come from inline JSON-Schema dicts in `_DEFAULT_TOOLS`; `_args_model_from_schema` converts each to a pydantic model at registration time.

### Default spec format

```python
{
    "name": "open_widget",
    "description": "Open / focus a specific widget on the user's current dashboard. Use when the user asks to 'show me the X widget'.",
    "parameters": {
        "type": "object",
        "properties": {"widget_id": {"type": "string", "description": "uuid of the widget to focus."}},
        "required": ["widget_id"],
    },
}
```

Each spec is fed into `_make_tool` from [`client_side`](client_side.md), so the dashboard surface is structurally identical to any other client-side surface — only the spec list differs.

## Side effects

None server-side. Every dashboard tool emits a `client_side_tool_call` interrupt (`{name, arguments}`) and pauses the LangGraph run. Workspace executes the action in the UI and resumes the run by posting the tool result — typically a small `{ok: true}` envelope or an error.

## Config

`[agent.tool_source_config.dashboard]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `tools` | list of `{name, description, parameters}` dicts | `_DEFAULT_TOOLS` (the four above) | Replace to expose a different surface; each spec passes through `_args_model_from_schema`. |

The `parameters` block uses JSON-Schema shape (`{type: "object", properties: {...}, required: [...]}`) — only `string`, `integer`, `number`, `boolean`, `array`, `object` map to concrete Python types; everything else falls back to `Any`.

## Related

- [`client_side` tool source](client_side.md) — the underlying interrupt mechanism (every dashboard tool is just a client-side declaration with a dashboard spec).
- [`workspace_mcp` tool source](workspace_mcp.md) — sibling that exposes the user's enabled MCP tools the same way.
- [Operating: configuration](../../../operating/configuration.md).
