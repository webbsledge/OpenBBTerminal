# `openbb_agent_server.plugins.tools.memory_recall`

Recall durable facts and preferences the current user has accumulated across prior conversations. Wraps a single `MemoryStore` instance as one `recall_user_memory` LangChain tool. Always principal-scoped via the active `RunContext` — cannot leak data across users.

**Source:** [`openbb_agent_server/plugins/tools/memory_recall.py`](../../../../openbb_agent_server/plugins/tools/memory_recall.py)

## Classes

### `MemoryRecallToolSource`

Plugin entry-point name: `recall_user_memory`. Constructor takes an optional `store: MemoryStore` — left `None` at import time and bound later by the app bootstrap through `_bind_store(store)`. `tools(ctx, config)` returns `[]` while `_store is None`; once bound, registers a single `StructuredTool`.

| Tool | Args | Returns |
| --- | --- | --- |
| `recall_user_memory` | `query: str` (what to recall about the user), `k: int = 8` (∈ [1, 32], how many memories to return) | `list[{id, text, kind, pinned, score}]` — each entry is a `Memory` row scored by the store. `kind` is the memory category; `pinned` marks user-locked entries. |

The implementation:

1. Reads `principal = run_context.current().principal` so the recall scope is the active user, not the agent process.
2. Calls `store.recall(principal=principal, query=query, k=k)` — typically a vector-search inside the configured memory backend.
3. Returns a JSON-friendly list of `{id, text, kind, pinned, score}`.

### Lifecycle

The tool source is unusual in that it is instantiated at module import time but bound to its `MemoryStore` later. The bootstrap flow:

1. Plugin discovery instantiates `MemoryRecallToolSource()` (with `store=None`).
2. The app bootstrap constructs the configured `MemoryStore` (SQLite, Postgres+pgvector, …) and calls `tool_source._bind_store(store)`.
3. Until `_bind_store` runs, `tools(ctx, config)` returns `[]` — the tool simply does not appear on the model's surface. This is deliberate: an unconfigured memory store should not silently fail at recall time; it should be absent entirely.

## Security

Principal-scoping is enforced inside `store.recall`. The tool itself only forwards what's in `RunContext.principal`, so swapping principals would require subverting the request-auth path, not the tool. The model has no way to ask the store for "all users' memories" — there is no such argument.

## Config

`[agent.tool_source_config.recall_user_memory]` is currently empty — the store is injected by the app bootstrap (`_bind_store`), not by per-call config.

The bound store is typically one of the backends documented in [`operating/memory.md`](../../../operating/memory.md): SQLite, Postgres + pgvector, or a custom subclass.

## Related

- [Operating: memory](../../../operating/memory.md) — the `MemoryStore` lifecycle and the backends shipped in the project.
- [`runtime/principal.py`](../../runtime/principal.md) — the per-run identity the recall is scoped to.
- [`runtime/context.py`](../../runtime/context.md) — `run_context.current()` access pattern.
