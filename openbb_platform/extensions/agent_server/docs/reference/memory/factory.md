# `openbb_agent_server.memory.factory`

Three factory functions that turn a provider name + optional model / config into a concrete embeddings / reranker / translator object. Called by `app/app.py` during server startup and re-used inside `SqliteMemoryStore` and the ingestion pipeline.

**Source:** [`openbb_agent_server/memory/factory.py`](../../../openbb_agent_server/memory/factory.py)

## `def make_embeddings(provider, *, model=None, config=None) -> Embeddings`

Construct an embeddings backend.

| `provider` | Returns | Default model |
| --- | --- | --- |
| `None` / `""` / `"hash"` / `"default"` | [`HashEmbeddings`](embeddings.md) (with a logged warning) | n/a |
| `"nvidia"` | `langchain_nvidia_ai_endpoints.NVIDIAEmbeddings` | `nvidia/nv-embed-v1` |
| `"nvidia-code"` | `NVIDIAEmbeddings` tuned for source code | `nvidia/nv-embedcode-7b-v1` |

NVIDIA paths require `NVIDIA_API_KEY` (env or `config["api_key"]`) and the `langchain-nvidia-ai-endpoints` package. `config` accepts `api_key`, `base_url`, `truncate` (defaults to `"END"`), `dimensions`, and — for `"hash"` — `dim`. Unknown provider names raise `ValueError`.

## `def make_reranker(provider, *, model=None, config=None) -> NvidiaReranker | None`

Construct a [`NvidiaReranker`](reranker.md), or return `None` when disabled.

- Empty / falsy `provider` → `None` (reranking is opt-in).
- `"nvidia"` → `NvidiaReranker(model=…, api_key=…, base_url=…, truncate=…, top_n=…)`.
- Anything else → `ValueError`.

Default model: `nv-rerank-qa-mistral-4b:1`. `top_n` is forwarded to the underlying `NVIDIARerank`; callers can still override per-call via `rerank(..., top_k=...)`.

## `def make_translator(provider, *, model=None, config=None) -> NvidiaTranslator | None`

Construct a [`NvidiaTranslator`](translation.md), or return `None` when disabled.

- Empty / falsy `provider` → `None`.
- `"nvidia"` → `NvidiaTranslator(model=…, api_key=…, base_url=…, temperature=…, max_tokens=…)`.
- Anything else → `ValueError`.

Default model: `nvidia/riva-translate-4b-instruct-v1_1`. `temperature` defaults to `0.0`; `max_tokens` defaults to `2048`.

## Provider entry-point lookup

The factories do NOT go through `runtime.registry.load` — they construct concrete classes directly. The provider names are hardcoded (only NVIDIA is built-in). Operators who want a different provider add a fresh factory function or wire a custom `Embeddings` instance into `SqliteMemoryStore` themselves; the abstraction here is intentionally narrow.

## TOML

```toml
[agent]
embeddings_provider = "nvidia"
embeddings_model    = "nvidia/nv-embed-v1"
embeddings_code_provider = "nvidia-code"
embeddings_code_model    = "nvidia/nv-embedcode-7b-v1"
reranker_provider   = "nvidia"
reranker_model      = "nv-rerank-qa-mistral-4b:1"
translation_provider = "nvidia"
translation_model   = "nvidia/riva-translate-4b-instruct-v1_1"

[agent.embeddings_config]
truncate = "END"

[agent.reranker_config]
top_n = 32
```

`NVIDIA_API_KEY` can live in env or under `[agent.embeddings_config.api_key]`. See [`operating/configuration.md`](../../operating/configuration.md#environment-variables) for the full env-var matrix.

## See also

- [`memory/embeddings.md`](embeddings.md) — the fallback.
- [`memory/reranker.md`](reranker.md) / [`memory/translation.md`](translation.md) — concrete NVIDIA clients.
- [`memory/sqlite_store.md`](sqlite_store.md) — consumer.
