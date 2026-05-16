# `openbb_agent_server.plugins.tools.python_module`

Discover LangChain tools at dotted-path locations and bind them into the agent. Use this when you have an existing Python package full of `@tool` decorated functions and want them registered without writing a `ToolSource` per surface.

**Source:** [`openbb_agent_server/plugins/tools/python_module.py`](../../../../openbb_agent_server/plugins/tools/python_module.py)

## Classes

### `PythonModuleToolSource`

Plugin entry-point name: `python_module`. Constructor takes `modules: Sequence[str]` — each entry is a `"package.module:attribute"` spec. `tools(ctx, config)` reads `config.get("modules", self._specs)`, resolves each spec, and flattens the result into a `list[BaseTool]`.

| Tool surface | Args | Returns |
| --- | --- | --- |
| **Whatever the referenced attributes export** | Per-tool, defined by each tool's own pydantic args schema | LangChain `BaseTool` instances. |

### Spec format

Each spec is a colon-separated `module:attribute` string. `_resolve(spec)` calls `importlib.import_module(module)` and `getattr(module, attr)`. A spec without `:` raises `ValueError("python_module tool spec must be 'pkg.mod:attribute', got ...")`.

The resolved attribute can be:

- A `BaseTool` instance — wrapped in a single-element list.
- A callable (factory) that produces a `BaseTool` / list / tuple when called with no arguments — invoked via a `contextlib.suppress(TypeError)` so factories that demand arguments simply pass through unchanged.
- A `list` or `tuple` of any of the above — flattened recursively.

Anything else raises `TypeError("python_module spec resolved to unsupported type <T>")`.

### Example

```toml
[agent.tool_sources]
my_package = "python_module"

[agent.tool_source_config.python_module]
modules = [
    "my_pkg.tools.public:list_of_tools",
    "my_pkg.tools.experimental:build_experimental_tools",
]
```

## Security

The model never controls the spec list — modules come from operator config only. The Python import itself runs at tool-build time, not at agent-call time, so import-time side effects are bounded to the launcher startup.

## Config

`[agent.tool_source_config.python_module]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `modules` | list[string] | `()` | Each entry is a `pkg.mod:attribute` spec; resolved at every `tools()` call so changes take effect on the next request. |

## Related

- [`writing-a-tool-source.md`](../../../developing/writing-a-tool-source.md) — write a proper `ToolSource` for richer config or lifecycle control.
- [`mcp_local` tool source](mcp_local.md) — for binding an existing MCP surface.
