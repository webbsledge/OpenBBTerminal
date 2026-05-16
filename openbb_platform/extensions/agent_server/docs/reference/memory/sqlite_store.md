# `openbb_agent_server.memory.sqlite_store`

SQLite-backed `MemoryStore` over `langchain_community.vectorstores.SQLiteVec`. The default and only built-in implementation. Optionally pairs an NVIDIA cross-encoder reranker over the ANN pool for higher-precision recall.

**Source:** [`openbb_agent_server/memory/sqlite_store.py`](../../../openbb_agent_server/memory/sqlite_store.py)

## Table layout

The store creates up to four tables in the same SQLite file:

| Table | Purpose |
| --- | --- |
| `memories_text` | Row store for text-embedded memories. Each row carries `text` + a JSON `metadata` blob (`memory_id`, `user_id`, `kind`, `pinned`, `source_trace_id`, `created_at`). |
| `memories_text_vec` | Companion `sqlite-vec` ANN index for `memories_text` (one row per text row, embedding column). |
| `memories_code` | Row store for code-embedded memories — created only when `code_embeddings` is passed to the constructor. |
| `memories_code_vec` | Companion ANN index for `memories_code`. |

Per-row `user_id` lives in `metadata` (a JSON column). Every read filters with `json_extract(metadata, '$.user_id') = ?` for cross-user isolation; the ANN candidate pool is over-fetched (4× the requested fanout) and filtered in Python to absorb the loss from rows belonging to other users.

## `class SqliteMemoryStore(MemoryStore)`

```python
SqliteMemoryStore(
    url: str,
    *,
    embeddings: Embeddings | None = None,
    code_embeddings: Embeddings | None = None,
    reranker: NvidiaReranker | None = None,
    rerank_fanout: int = 32,
)
```

| Arg | Purpose |
| --- | --- |
| `url` | `sqlite:///path/to/file.db` or `sqlite+aiosqlite:///path/to/file.db` — converted to a plain file path. `:memory:` is supported for tests. |
| `embeddings` | LangChain `Embeddings` for prose. Falls back to [`HashEmbeddings`](embeddings.md) when `None` (with a logged warning — recall quality is poor). |
| `code_embeddings` | Optional embeddings for code; when set the second table pair (`memories_code` / `memories_code_vec`) is created. Routed to by `kind` ending in `_code`. |
| `reranker` | Optional [`NvidiaReranker`](reranker.md). When set, `recall()` cross-encodes the top `rerank_fanout` ANN candidates and re-orders by relevance score. Falls back to ANN order on exception. |
| `rerank_fanout` | How wide to pull the ANN pool before reranking. Default 32. Higher = better recall, more reranker calls. |

### Hybrid retrieval

`recall()` is a four-step pipeline:

1. **ANN over-fetch** — `similarity_search_with_score(query, k=fanout*4)` against every configured store, then Python-filter by `user_id` down to `fanout` candidates per table.
2. **Pinned merge** — every row with `pinned = 1` is loaded and merged in with a boosted score of `1.0` (or its ANN score, whichever is higher).
3. **Rerank** (optional) — the top `fanout` after the merge plus every still-unseen pinned row is sent to the reranker as `[(memory_id, text), ...]`; rerank returns `[(memory_id, relevance_score), ...]` truncated to `k`.
4. **Fallback** — on any reranker exception, the cosine-only order is returned.

Distances from `SQLiteVec` are mapped to `[0, 1]` via `1 / (1 + distance)`.

### Concurrency

The constructor opens one `sqlite3.Connection` with `check_same_thread=False`, loads the `sqlite-vec` extension, and guards all writes with a process-wide `threading.Lock`. All public methods are `async` and route the sync sqlite work through `asyncio.to_thread` so the event loop stays free.

## `def delete_all_for_user(conn, tables, user_id) -> int`

Right-to-erasure helper. Drops every row (and matching `_vec` row) in `tables` whose `metadata.user_id` matches. Called by `delete_all_for_user(principal)` and by `app/router.py`'s `DELETE /v1/me`.

## TOML

```toml
[agent]
embeddings_provider = "nvidia"
embeddings_model    = "nvidia/nv-embed-v1"
embeddings_code_provider = "nvidia-code"
embeddings_code_model    = "nvidia/nv-embedcode-7b-v1"
reranker_provider   = "nvidia"
reranker_model      = "nv-rerank-qa-mistral-4b:1"
rerank_fanout       = 32
data_dir            = "~/.openbb_platform/agent"  # → data_dir/memory.db
```

The DB file lives at `{data_dir}/memory.db` by default; override via `OPENBB_AGENT_MEMORY_URL` for shared SQLite or to point at an entirely separate file.

## See also

- [`memory/store.md`](store.md) — the ABC and `Memory` record.
- [`memory/factory.md`](factory.md) — how `embeddings` / `reranker` get constructed.
- [`operating/memory.md`](../../operating/memory.md) — operational guide.
