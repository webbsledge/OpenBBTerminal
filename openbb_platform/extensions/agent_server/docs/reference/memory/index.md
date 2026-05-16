# `openbb_agent_server.memory`

Per-user cross-thread memory subsystem. Long content from each turn is ingested into a SQLite + `sqlite-vec` store; a post-turn extractor distils durable facts from the conversation; retrieval is hybrid (cosine ANN + optional NVIDIA cross-encoder rerank), with pinned rows always merged in. Every read and write is `user_id`-partitioned and gated on the `memory:read` / `memory:write` scopes.

**Source:** [`openbb_agent_server/memory/__init__.py`](../../../openbb_agent_server/memory/__init__.py)

## Pages

| Page | What it covers |
| --- | --- |
| [`store.md`](store.md) | `MemoryStore` ABC + the `Memory` record consumed by every retriever, writer, and ingestion helper. |
| [`sqlite_store.md`](sqlite_store.md) | `SqliteMemoryStore` — the default SQLite + `sqlite-vec` implementation, including table layout, per-user metadata isolation, and the four-step hybrid retrieval pipeline. |
| [`embeddings.md`](embeddings.md) | `HashEmbeddings` — the deterministic feature-hashed fallback used in tests and as the dev default. Plus the `cosine` helper. |
| [`factory.md`](factory.md) | `make_embeddings` / `make_reranker` / `make_translator` — turn a provider name + config into a concrete client. |
| [`reranker.md`](reranker.md) | `NvidiaReranker` — async cross-encoder reranker over NVIDIA NIM. |
| [`translation.md`](translation.md) | `NvidiaTranslator` — translation client (Riva-translate by default) used to normalise non-English ingested chunks. |
| [`classifier.md`](classifier.md) | `looks_like_code` — extension / MIME / token-density heuristic for code vs. prose routing. |
| [`ingestion.md`](ingestion.md) | `ingest_request_context` — chunks long uploads + human messages and writes them to the store at the start of each turn. |
| [`retrievers.md`](retrievers.md) | LangChain `BaseRetriever` facades over `MemoryStore` and `WidgetDataStore`. |
| [`writer.md`](writer.md) | Post-turn memory writer — extracts durable facts from the AI reply via `EXTRACTOR_SYSTEM_PROMPT` and fire-and-forgets the writes. |

## End-to-end flow

1. **`POST /v1/query` lands.** The router authenticates, builds the `RunContext`, and calls `ingest_request_context(...)` (see [`ingestion.md`](ingestion.md)). Long uploads + long human messages are chunked, optionally translated, and written to the store with `kind="context_text"` or `"context_code"`.
2. **Agent runs.** The `recall_user_memory` tool exposes `MemoryStoreRetriever` (see [`retrievers.md`](retrievers.md)) to the agent; recall is principal-scoped, top-`k`, with pinned rows always merged in.
3. **Final answer streams.** The `memory_writer` middleware (post-turn) runs `EXTRACTOR_SYSTEM_PROMPT` over the `(human, ai)` transcript (see [`writer.md`](writer.md)) and writes any extracted durable facts as `kind="fact"`.

## Scopes summary

| Entry point | Required scope |
| --- | --- |
| `MemoryStore.write` | `memory:write` |
| `ingest_request_context` (per-turn chunked uploads) | `memory:write` |
| Post-turn `memory_writer` middleware | `memory:write` |
| `GET /v1/memory` | `memory:read` |
| `PATCH /v1/memory/{id}` / `DELETE /v1/memory/{id}` | `memory:write` |
| `recall_user_memory` tool / `MemoryStore.recall` | (none — any authenticated principal) |

## See also

- [`operating/memory.md`](../../operating/memory.md) — operational guide.
- [`guides/memory-and-recall.md`](../../guides/memory-and-recall.md) — end-to-end walkthrough.
- [`runtime/widget_store.md`](../runtime/widget_store.md) — the sibling widget-row store (`WidgetDataRetriever` consumes this).
- [`runtime/principal.md`](../runtime/principal.md) — the `memory:read` / `memory:write` scopes that gate every entry point.
