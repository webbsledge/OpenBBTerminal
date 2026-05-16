# `openbb_agent_server.plugins.tools.mcp_http`

Connect to an **already running** MCP server over HTTP, SSE, or WebSocket and surface its tools to the agent. Pair-piece to [`mcp_local`](mcp_local.md), which spawns the server itself; pick `mcp_http` when you have a centrally hosted MCP fleet that multiple agents share.

**Source:** [`openbb_agent_server/plugins/tools/mcp_http.py`](../../../../openbb_agent_server/plugins/tools/mcp_http.py)

## Classes

### `HttpMcpToolSource`

Plugin entry-point name: `mcp_http`. Constructor takes `url`, `transport`, `headers`, `server_name`, `config_file`. `tools(ctx, config)` builds the connection spec and hands off to `langchain_mcp_adapters.client.MultiServerMCPClient`, returning `await client.get_tools()`.

| Tool surface | Args | Returns |
| --- | --- | --- |
| **All tools exported by the remote MCP server** | Per-tool — whatever `tools/list` publishes | LangChain `BaseTool` instances. |

### Transport selection

`_VALID_TRANSPORTS = {"streamable_http", "sse", "websocket"}`. `_TRANSPORT_ALIASES` maps the dash-form (`streamable-http`) and `ws` to canonical underscored values. Resolution order (later wins):

1. `[mcp].transport` from the TOML cascade.
2. Constructor `transport`.
3. Per-call `config["transport"]`.

Falls back to `"streamable_http"` if none is set.

### URL resolution

`_build_url(host, port, transport)` constructs `http://<host>:<port>/sse` for SSE or `http://<host>:<port>/mcp` for the other transports. If `host` already starts with `http://` / `https://`, the scheme is preserved and `port` is appended only when not already present in the host string.

Order:

1. `config["url"]` (per-call).
2. Constructor `url`.
3. Composed from `[mcp].host` + `[mcp].port`. Without those, raises `RuntimeError`.

### Config-file resolution

Same env-var cascade as `mcp_local`:

`OPENBB_AGENT_MCP_CONFIG` → `OPENBB_MCP_CONFIG` → `OPENBB_AGENT_CONFIG` → `OPENBB_API_CONFIG` → `OPENBB_CONFIG`

The cascade is used for `[mcp].host`, `[mcp].port`, `[mcp].transport`, and `[mcp].spec.headers` (a dict of default headers).

### Header layering

Headers are stacked in this order (later wins):

1. Constructor `headers`.
2. `[mcp].spec.headers` from the cascade.
3. Per-call `config["headers"]`.
4. `ctx.api_keys` — each entry is set with key `X-OPENBB-<NAME>` and used as `setdefault` (an explicit header above the cascade wins).

## Security

- API keys travel as `X-OPENBB-*` headers, set via `setdefault` so explicit operator-supplied headers take priority.
- The model never controls the URL, transport, or headers — those are operator config.
- The MCP server is responsible for its own auth; the source simply forwards the headers it was given.

## Config

`[agent.tool_source_config.mcp_http]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `url` | string | from `[mcp].host` + `[mcp].port` | Per-call override or constructor default. |
| `transport` | string | `[mcp].transport` then `"streamable_http"` | `streamable_http` / `sse` / `websocket` (or the dash form). |
| `headers` | dict[string, string] | `{}` | Layered into the request headers; `X-OPENBB-<KEY>` from `ctx.api_keys` is added with `setdefault`. |
| `config_file` | string | from env-var cascade | Overrides the cascade for the `[mcp]` lookup. |

## Related

- [`mcp_local` tool source](mcp_local.md) — sibling that spawns a local subprocess.
- [`workspace_mcp` tool source](workspace_mcp.md) — surfaces Workspace's enabled MCP tools to the agent.
- [Operating: configuration](../../../operating/configuration.md) — the `[mcp]` section.
