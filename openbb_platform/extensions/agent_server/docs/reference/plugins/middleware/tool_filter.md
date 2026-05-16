# `openbb_agent_server.plugins.middleware.tool_filter`

Drop tools by name from the model's bound tool list before each `model_call`. Used to suppress DeepAgents' generic filesystem suite (we route file access through `pdf_extract` / `widget_data` instead).

**Source:** [`openbb_agent_server/plugins/middleware/tool_filter.py`](../../../../openbb_agent_server/plugins/middleware/tool_filter.py)

## Classes

### `ToolFilterMiddlewareFactory`

Plugin entry-point name: `tool_filter`.

`build(ctx, config) -> AgentMiddleware` returns a per-run middleware that removes any tool whose name is in `config["excluded"]`.

| Config key | Type | Default | Effect |
| --- | --- | --- | --- |
| `excluded` | `list[str] \| set[str]` | `{"ls", "execute", "read_file", "write_file", "edit_file", "glob", "grep"}` | Names to drop. When `config["excluded"]` is unset entirely (NOT `[]`), the default set is used; an explicit empty list disables filtering. |

The default set targets DeepAgents' built-in filesystem tools. To keep them while filtering something else, supply a fresh set:

```toml
[agent.tool_source_config.tool_filter]
excluded = ["execute", "my_custom_tool"]
```
