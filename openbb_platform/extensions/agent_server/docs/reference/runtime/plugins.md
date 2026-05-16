# `openbb_agent_server.runtime.plugins`

Plugin ABCs / protocols. Every swappable piece of the runtime — auth, model, tools, middleware, sub-agents, checkpointer — inherits from one of these and is discovered by name via Python entry points (see [plugin-system.md](../../developing/plugin-system.md)).

**Source:** [`openbb_agent_server/runtime/plugins.py`](../../../openbb_agent_server/runtime/plugins.py)

Every ABC carries a `name: str` class attribute that **must** match the entry-point name used to register the plugin. The runtime registry resolves names to instances by reading the matching entry-point group and calling the entry's class with no constructor args.

## `class AuthBackend(ABC)`

Resolve a request's credentials into a [`UserPrincipal`](principal.md).

```python
class AuthBackend(ABC):
    name: str
    async def authenticate(self, request: Request) -> UserPrincipal: ...
```

Entry-point group: `openbb_agent_server.auth`. See [`plugins/auth/`](../plugins/auth/index.md) for built-ins.

## `class ModelProvider(ABC)`

Build a LangChain `BaseChatModel` for one run.

```python
class ModelProvider(ABC):
    name: str
    def build(self, ctx: RunContext, config: dict[str, Any]) -> Any: ...
```

`config` is the merged `[agent.model.config]` for this profile. The return value must support the `langchain_core.language_models.BaseChatModel` surface (`bind_tools`, `astream`, etc.). Entry-point group: `openbb_agent_server.models`.

## `class ToolSource(ABC)`

Yield the agent's LangChain tools for one run.

```python
class ToolSource(ABC):
    name: str
    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[Any]: ...
```

`tools()` is called once per run, with full access to `RunContext` (uploaded files, widgets, principal, api_keys). The returned list is concatenated onto the agent's tool surface. Entry-point group: `openbb_agent_server.tools`.

## `class SubAgentSpec(Protocol)`

Duck-typed declaration of one DeepAgents sub-agent. **Protocol, not ABC** — inheriting from `SubAgentSpec` adds nothing; any class with these attributes satisfies the protocol.

```python
class SubAgentSpec(Protocol):
    name: str
    description: str
    system_prompt: str
    tools: tuple[str, ...]
    model: str | None
```

| Field | Purpose |
| --- | --- |
| `name` | Sub-agent identifier; the parent agent calls `task(name=<this>, …)`. |
| `description` | One-paragraph description shown to the parent for tool-choice. |
| `system_prompt` | The sub-agent's instructions. |
| `tools` | Tool names to inherit from the parent surface. Names not present on the parent are silently dropped. |
| `model` | Optional model override. `None` → inherit parent. `str` → re-resolve through the registry. Or a fully-built `BaseChatModel`. |

Entry-point group: `openbb_agent_server.subagents`. See [`plugins/subagents/`](../plugins/subagents/index.md).

## `class Middleware(ABC)`

Per-run factory that returns a `langchain.agents.middleware.types.AgentMiddleware`.

```python
class Middleware(ABC):
    name: str
    def build(self, ctx: RunContext, config: dict[str, Any]) -> Any: ...
```

The returned middleware hooks `wrap_model_call` / `wrap_tool_call` (or their async `awrap_*` variants). `build()` is called every run so middleware state is per-run, not per-process. Entry-point group: `openbb_agent_server.middleware`.

## `class CheckpointerProvider(ABC)`

Build and lifecycle-manage the LangGraph checkpointer for the whole server (NOT per-run).

```python
class CheckpointerProvider(ABC):
    name: str
    async def open(self, settings: Any) -> Any: ...
    async def close(self, saver: Any) -> None: ...
```

`open()` returns a saver that's used for the lifetime of the FastAPI app; `close()` tears down connections / files at shutdown. Entry-point group: `openbb_agent_server.checkpointers`. Built-ins: `inmemory`, `sqlite`, `postgres`.
