# `openbb_agent_server.plugins.tools.workspace_mcp`

Surface the user's enabled MCP tools — the ones they have connected through Workspace itself — back to the agent so the model can call them. Each tool becomes a client-side stub: the run pauses on invocation and Workspace runs the actual MCP call against the user's own MCP server, then resumes the run with the result.

**Source:** [`openbb_agent_server/plugins/tools/workspace_mcp.py`](../../../../openbb_agent_server/plugins/tools/workspace_mcp.py)

## Classes

### `WorkspaceMcpToolSource`

Plugin entry-point name: `workspace_mcp`. No constructor arguments. `tools(ctx, config)` iterates `ctx.tools` (the snapshot of MCP tools Workspace forwarded with the request envelope) and builds one `StructuredTool` per entry through the local `_make_tool` helper.

| Tool surface | Args | Returns |
| --- | --- | --- |
| **One tool per entry in `ctx.tools`** | Whatever the upstream MCP server declared via its `input_schema` (parsed by `client_side._args_model_from_schema` into a pydantic model) | Whatever Workspace POSTs back as the resumed tool result after invoking the MCP server. |

Entries that are missing `name` or have an invalid spec are silently skipped — the tool source never raises on a malformed entry. This keeps the agent robust against partial Workspace state.

### Spec format

Each entry in `ctx.tools` is a dict:

```python
{
    "name": "fetch_recent_emails",
    "server_id": "gmail",       # optional, default "workspace"
    "description": "...",        # optional, default "Workspace MCP tool: <name> (server=<id>)."
    "input_schema": {            # optional, default {"type": "object", "properties": {}}
        "type": "object",
        "properties": {"label": {"type": "string"}},
    },
}
```

The registered tool is named `<WORKSPACE_MCP_TOOL_PREFIX><server_id>:<name>` — the prefix lets the adapter route the interrupt back to the correct upstream MCP server when there are multiple.

### Interrupt payload

When the model invokes a Workspace MCP tool, `_client_call` calls `interrupt(...)` with:

```python
{
    "type": "workspace_mcp_tool_call",
    "server_id": server_id,
    "name": raw_name,
    "arguments": dict(kwargs),
}
```

LangGraph pauses the run; Workspace executes the MCP call against the named upstream server and resumes the run with the result.

## Security

- The model controls only the arguments — server id and tool name are pinned by the spec.
- Argument validation runs through the auto-generated pydantic model before the interrupt.
- The server never executes the MCP call itself; it cannot leak secrets the user holds inside their MCP servers.
- Per-entry exception handling (`except (ValueError, KeyError, TypeError)`) means a malformed entry drops out instead of crashing tool registration.

## Config

`[agent.tool_source_config.workspace_mcp]` is currently empty — the tool surface comes entirely from `ctx.tools`, which Workspace populates per-request.

## Related

- [`client_side` tool source](client_side.md) — the underlying interrupt mechanism (`_args_model_from_schema` is shared).
- [`mcp_local` tool source](mcp_local.md), [`mcp_http` tool source](mcp_http.md) — server-side equivalents (the agent itself talks to an MCP server).
- [`protocol/adapter`](../../runtime/index.md) — `WORKSPACE_MCP_TOOL_PREFIX` definition.
