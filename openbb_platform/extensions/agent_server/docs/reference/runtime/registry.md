# `openbb_agent_server.runtime.registry`

Entry-point-based plugin loader. Reads `importlib.metadata.entry_points(group=…)` to discover plugins, looks them up by name, and instantiates with the per-plugin config (with unknown keys dropped + logged so a misplaced TOML key doesn't take the whole server down).

**Source:** [`openbb_agent_server/runtime/registry.py`](../../../openbb_agent_server/runtime/registry.py)

## Functions

### `def load(group, name, config=None) -> Any`

```python
def load(group: str, name: str, config: dict[str, Any] | None = None) -> Any
```

Load and instantiate one plugin from entry-point `group` named `name`. Raises `KeyError` with the list of available names if `name` isn't registered. No caching — each call hits `entry_points(...)` fresh; plugins are typically constructed once at app-startup so the cost is one entry-point scan per server boot.

| Arg | Purpose |
| --- | --- |
| `group` | Entry-point group, e.g. `openbb_agent_server.auth`, `openbb_agent_server.models`, `openbb_agent_server.tools`. |
| `name` | Plugin name (the `name = "..."` attribute the plugin's class declares). |
| `config` | Kwargs forwarded to `cls(**config)`. |

### Unknown-key handling

Before calling `cls(**config)`, the loader inspects `cls.__init__`'s signature:

- If `__init__` declares `**kwargs`, every key is accepted unconditionally.
- Otherwise unknown keys are popped from the config (logged at WARNING) and the remaining kwargs are passed. The warning prompts the operator to check whether the key is misplaced — a common error is putting `tool_sources` under `[model.config]` when it should be at `[agent]`.

### `def available(group) -> list[str]`

Return a sorted list of installed plugin names in `group`. Used by the `keys` CLI's auth-backend selector and by tests to assert that the expected plugins ship.

## Entry-point groups

| Group | Source-of-truth ABC / Protocol | Built-ins |
| --- | --- | --- |
| `openbb_agent_server.auth` | `AuthBackend` | `none`, `bearer_static`, `api_key_table`, `oidc_jwt`, `openbb_workspace`. |
| `openbb_agent_server.models` | `ModelProvider` | `anthropic`, `bedrock`, `fake`, `google_genai`, `groq`, `nvidia`, `openai`, `openai_compat`, `snowflake`, `vertex`. |
| `openbb_agent_server.tools` | `ToolSource` | Every plugin under `plugins/tools/`. |
| `openbb_agent_server.middleware` | `Middleware` | Every plugin under `plugins/middleware/`. |
| `openbb_agent_server.subagents` | `SubAgentSpec` Protocol | `analyst`, `charter`, `pdf_reader`, `researcher`. |
| `openbb_agent_server.checkpointers` | `CheckpointerProvider` | `inmemory`, `sqlite`, `postgres`. |

See [`runtime/plugins.md`](plugins.md) for the ABCs and [`developing/plugin-system.md`](../../developing/plugin-system.md) for how to ship your own.

## Module helpers

### `def _eps(group) -> dict[str, EntryPoint]`

Dict-by-name shortcut over `entry_points(group=...)`. Internal.

### `def _accepted_kwargs(cls) -> set[str] | None`

Inspect `cls.__init__` and return the kwarg names. Returns `None` to mean "accepts arbitrary `**kwargs`". Used by `load()` to decide whether to drop unknown config keys.

## See also

- [`runtime/plugins.md`](plugins.md) — the ABCs / Protocols `load()` instantiates.
- [`developing/plugin-system.md`](../../developing/plugin-system.md) — how to register a plugin.
