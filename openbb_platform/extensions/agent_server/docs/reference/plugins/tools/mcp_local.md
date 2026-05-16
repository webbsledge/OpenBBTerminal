# `openbb_agent_server.plugins.tools.mcp_local`

Spawn the `openbb-mcp` CLI as a subprocess over stdio and surface every tool it exports as a LangChain `BaseTool`. The complete OpenBB Platform command surface (`obb.equity.price.historical`, `obb.economy.gdp.real`, etc.) becomes addressable from the agent in one bind — no per-command wiring required.

**Source:** [`openbb_agent_server/plugins/tools/mcp_local.py`](../../../../openbb_agent_server/plugins/tools/mcp_local.py)

## Classes

### `LocalMcpToolSource`

Plugin entry-point name: `mcp_local`. Constructor takes `command`, `args`, `env`, `config_file`. `tools(ctx, config)` resolves the command, builds the subprocess env, and hands off to `langchain_mcp_adapters.client.MultiServerMCPClient` which returns `await client.get_tools()`.

| Tool surface | Args | Returns |
| --- | --- | --- |
| **All tools exported by `openbb-mcp`** | Per-tool — whatever the MCP server publishes via its `tools/list` schema | LangChain `BaseTool` instances, one per MCP tool. |

Every tool returned by `get_tools()` is a real LangChain tool with the same async invoke contract — they show up alongside the rest of the agent's bound tools and are indistinguishable to the model.

### Command resolution

`_resolve_command` looks up the binary in three tiers:

1. If the value contains a path separator, treat it as a literal path (must `is_file()`).
2. `shutil.which(command)`.
3. The sibling directory of the running Python (`sys.executable.parent / command`) — this catches venv-installed CLIs.

A missing command raises `RuntimeError` with an install hint pointing at `openbb-mcp-server`.

### Config-file resolution

`_resolve_config_file` walks an env-var cascade in order:

`OPENBB_AGENT_MCP_CONFIG` → `OPENBB_MCP_CONFIG` → `OPENBB_AGENT_CONFIG` → `OPENBB_API_CONFIG` → `OPENBB_CONFIG`

The first set value wins. If the cascade resolves to a path, the source re-walks the TOML via `bootstrap_launcher_config(explicit_path=...)` to fetch the `[mcp]` table (purely informational — for logging). The path is then forwarded to the subprocess via `--config-file` so the spawned `openbb-mcp` reads the same configuration the launcher saw.

### Subprocess argv

`DEFAULT_ARGS = ("--transport", "stdio")` — the CLI defaults to `streamable-http` when run with no flags, so the source always forces `--transport stdio` (overriding any `[mcp].transport` setting) because it owns the subprocess pipe. `_ensure_arg` appends `--config-file <path>` unless the flag is already present.

### Environment merge

Subprocess env is layered (later wins): `os.environ` → constructor `env` → per-call `config["env"]` → `ctx.api_keys`. The api keys are forwarded as plain env vars so the subprocess providers can read them.

## Security

- The model never controls the subprocess argv — they come from constructor / config / cascade only.
- `ctx.api_keys` flows in as env vars; if the subprocess providers read environment-variable secrets, this is the path they take.
- The subprocess inherits the launcher's stdout/stderr — log scraping is the operator's responsibility.

## Config

`[agent.tool_source_config.mcp_local]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `command` | string | `"openbb-mcp"` | Absolute path or PATH-resolvable name. |
| `args` | list[string] | `["--transport", "stdio"]` | `--transport stdio` is always forced; `--config-file` is appended if a cascade resolves. |
| `env` | dict[string, string] | `{}` | Layered into the subprocess env before `ctx.api_keys`. |
| `config_file` | string | `None` | Overrides the env-var cascade. |

## Related

- [`mcp_http` tool source](mcp_http.md) — sibling that connects to an already-running MCP server.
- [`workspace_mcp` tool source](workspace_mcp.md) — surface the user's enabled MCP tools through Workspace's MCP bridge.
- [Operating: configuration](../../../operating/configuration.md) — the `[mcp]` section.
