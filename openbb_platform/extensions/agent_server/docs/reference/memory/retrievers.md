# `openbb_agent_server.memory.retrievers`

Two LangChain `BaseRetriever` adapters that expose the agent server's stores under the standard retriever surface, so sub-agents and skills can compose them with LangChain's retrieval-chain primitives.

**Source:** [`openbb_agent_server/memory/retrievers.py`](../../../openbb_agent_server/memory/retrievers.py)

## `class MemoryStoreRetriever(BaseRetriever)`

Retriever-shaped facade over [`MemoryStore.recall`](store.md).

```python
MemoryStoreRetriever(
    *,
    store: MemoryStore,
    principal: UserPrincipal,
    k: int = 8,
)
```

| Field (PrivateAttr) | Purpose |
| --- | --- |
| `store` | The underlying `MemoryStore`. |
| `principal` | The user to scope `recall` to. Captured at construction; never reads from the contextvar. |
| `k` | Top-`k` to fetch per call. `max(1, int(k))`. |

### Returned `Document` shape

| `Document` attr | Source |
| --- | --- |
| `page_content` | `memory.text` |
| `metadata.memory_id` | `memory.memory_id` |
| `metadata.user_id` | `memory.user_id` |
| `metadata.kind` | `memory.kind` |
| `metadata.pinned` | `memory.pinned` |
| `metadata.source_trace_id` | `memory.source_trace_id` |
| `metadata.score` | `memory.score` |

Both `_get_relevant_documents` (sync) and `_aget_relevant_documents` (async) are implemented. The sync path delegates to `asyncio.run(self._arun(query))`.

## `class WidgetDataRetriever(BaseRetriever)`

Retriever-shaped facade over [`WidgetDataStore.search`](../runtime/widget_store.md).

```python
WidgetDataRetriever(
    *,
    store: WidgetDataStore,
    principal: UserPrincipal,
    conversation_id: str,
    k: int = 8,
    widget_uuid: str | None = None,
)
```

| Field (PrivateAttr) | Purpose |
| --- | --- |
| `store` | The `WidgetDataStore`. |
| `principal` | Owner. |
| `conversation_id` | Scope. Searches are partitioned by conversation; pass the per-run id. |
| `k` | Top-`k`. |
| `widget_uuid` | Optional filter to one widget's rows. |

### Returned `Document` shape

The store returns hits as `{"row": {...}, "score": float, "widget_uuid": ..., "widget_name": ...}`. The retriever flattens each row dict into a pipe-joined string for `page_content` (`"col1: v1 | col2: v2"`) and lifts the rest onto `metadata`:

| `Document` attr | Source |
| --- | --- |
| `page_content` | Flattened `row` dict — `" | ".join(f"{k}: {v}" for k,v in row.items() if v is not None)`. |
| `metadata.widget_uuid` | hit's `widget_uuid` |
| `metadata.widget_name` | hit's `widget_name` |
| `metadata.score` | hit's score (cosine-style; `1.0` for substring-match fallback) |
| `metadata.row` | The raw row dict, so downstream code can read fields by name. |

## Usage

```python
from openbb_agent_server.memory.retrievers import MemoryStoreRetriever

retriever = MemoryStoreRetriever(store=memory_store, principal=ctx.principal, k=6)
docs = await retriever.ainvoke("user's preferred lookback window")
```

These are the entry points DeepAgents' skills middleware uses to compose retrieval against the agent's stores without leaking the principal abstraction.

## See also

- [`memory/store.md`](store.md) — underlying ABC.
- [`runtime/widget_store.md`](../runtime/widget_store.md) — widget-row store.
