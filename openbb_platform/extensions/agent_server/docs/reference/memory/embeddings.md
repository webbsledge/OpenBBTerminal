# `openbb_agent_server.memory.embeddings`

`HashEmbeddings` — the zero-config feature-hashed embedder used as a fallback when no real provider is configured — plus a `cosine` similarity helper. Both are deterministic and dependency-free, so the test suite can exercise the recall pipeline without an NVIDIA API key.

**Source:** [`openbb_agent_server/memory/embeddings.py`](../../../openbb_agent_server/memory/embeddings.py)

## `class HashEmbeddings(Embeddings)`

LangChain `Embeddings` implementation using feature hashing over unigrams + bigrams.

```python
HashEmbeddings(*, dim: int = 256)
```

| Arg | Type | Default | Purpose |
| --- | --- | --- | --- |
| `dim` | `int` | `256` | Output vector dimension. Must be positive; lower values increase collisions. |

### Algorithm

1. Lowercase, whitespace-split into unigrams; append `f"{w_i}_{w_{i+1}}"` bigrams.
2. For each feature, hash via `blake2s(feature, digest_size=8)`.
3. Take the bottom 4 bytes as the bucket index (`% dim`); the next byte's LSB as a `+1` / `-1` sign.
4. Sum signed counts, L2-normalise.

The hash is deterministic across processes — embeddings are stable across restarts so cached vectors keep matching new queries.

### Methods

| Method | Purpose |
| --- | --- |
| `embed_documents(texts) -> list[list[float]]` | Batch-embed a list of texts. |
| `embed_query(text) -> list[float]` | Single-text embed; identical math to `embed_documents`. |

### When to use

- **Tests** — the default in CI; no network, no API key.
- **Local dev fallback** — `SqliteMemoryStore` defaults to `HashEmbeddings` when `embeddings=None` and logs a warning. Semantic recall quality is poor (token overlap + bigram chance); for production set `EMBEDDINGS_PROVIDER=nvidia` with a real `NVIDIA_API_KEY`.

Never run a production tenant on `HashEmbeddings` — there is no semantic understanding, and word-order sensitivity is partial.

## `def cosine(a, b) -> float`

```python
cosine(a: Sequence[float], b: Sequence[float]) -> float
```

Cosine similarity, robust to length mismatch (uses the shorter prefix of `a` / `b`). Returns `0.0` for empty inputs. Used by the test suite and the few callers that need to score two embeddings directly without going through a vector store.

## See also

- [`memory/sqlite_store.md`](sqlite_store.md) — embeds via this class by default.
- [`memory/factory.md`](factory.md) — `make_embeddings("hash")` / `make_embeddings("nvidia")`.
