# `openbb_agent_server.memory.reranker`

Async cross-encoder reranker backed by an NVIDIA NIM model. Wraps `langchain_nvidia_ai_endpoints.NVIDIARerank` with lazy construction, principal-agnostic scoring, and best-effort score parsing across NIM response-shape changes.

**Source:** [`openbb_agent_server/memory/reranker.py`](../../../openbb_agent_server/memory/reranker.py)

## `class NvidiaReranker`

```python
NvidiaReranker(
    *,
    model: str = "nv-rerank-qa-mistral-4b:1",
    api_key: str | None = None,
    base_url: str | None = None,
    truncate: str = "END",
    top_n: int | None = None,
)
```

| Arg | Type | Default | Purpose |
| --- | --- | --- | --- |
| `model` | `str` | `"nv-rerank-qa-mistral-4b:1"` | NIM model id. |
| `api_key` | `str \| None` | `os.environ["NVIDIA_API_KEY"]` | API key. Required at first `rerank()` call, not at construction. |
| `base_url` | `str \| None` | `None` | Optional NIM endpoint override. |
| `truncate` | `str` | `"END"` | How the NIM truncates over-long inputs (`"END"` / `"START"` / `"NONE"`). |
| `top_n` | `int \| None` | `None` | Default `top_n` for the underlying `NVIDIARerank` client. Per-call `top_k` still wins. |

The underlying `NVIDIARerank` client is built lazily on first `rerank()` call — construction is cheap but does no network IO.

### `async def rerank(query, candidates, *, top_k=None) -> list[tuple[str, float]]`

Re-rank `candidates: Sequence[tuple[str, str]]` against `query`. Each candidate is `(memory_id, text)`. The returned list is `(memory_id, relevance_score)`, sorted descending, truncated to `top_k` if provided.

| Behaviour | Detail |
| --- | --- |
| Empty `candidates` | Returns `[]` without touching the network. |
| Empty / whitespace `query` | Returns the input order with `0.0` scores (no discriminative signal). |
| `acompress_documents` available | Awaited directly (async path). |
| Sync-only client | Falls back to `asyncio.to_thread(self._client.compress_documents, …)`. |
| Score missing / non-numeric | Falls back to `metadata["score"]` then `0.0`. |

The `_rerank_id` metadata key is stashed on each `Document` before the rerank call so the original `memory_id` can be recovered after the underlying library re-orders the documents.

### Used by

- [`SqliteMemoryStore.recall`](sqlite_store.md#hybrid-retrieval) — passes `rerank_fanout` candidates through this client and re-orders by relevance.

## See also

- [`memory/factory.md`](factory.md) — construction.
- [`operating/memory.md`](../../operating/memory.md) — operational guide and tuning notes.
