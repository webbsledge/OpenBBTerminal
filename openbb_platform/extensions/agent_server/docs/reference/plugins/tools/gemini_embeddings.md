# `openbb_agent_server.plugins.tools.gemini_embeddings`

Text-vector embeddings via Google GenAI as an alternative to NVIDIA NIM's embedding models. One tool, `embed_text`; same per-run lifecycle as the NVIDIA embedders so it can be hot-swapped without changing the rest of the pipeline.

**Source:** [`openbb_agent_server/plugins/tools/gemini_embeddings.py`](../../../../openbb_agent_server/plugins/tools/gemini_embeddings.py)

## Classes

### `GeminiEmbeddingsToolSource`

Plugin entry-point name: `gemini_embeddings`. Constructor takes `api_key`, `default_model`, `default_task_type`, `default_output_dimensionality`. The constructor validates the task type against `_VALID_TASK_TYPES` and requires `default_output_dimensionality >= 1` when set. `tools(ctx, config)` resolves the api key (`ctx.api_keys["GOOGLE_API_KEY"]` → `ctx.api_keys["GEMINI_API_KEY"]` → config → constructor); raises `RuntimeError` at tool-build time if absent.

| Tool | Args | Returns |
| --- | --- | --- |
| `embed_text` | `texts: list[str]` (strings to embed), `model: str \| None` (override the configured default), `task_type: str \| None` (one of the values below), `output_dimensionality: int \| None` (truncate vectors; model-dependent) | `{vectors, model, task_type, dimensions, count}` — `vectors` is a `list[list[float]]` matching the input order. Empty `texts` short-circuits to `{vectors: [], model, dimensions: 0}`. |

### Task types

`task_type` tunes the embedding geometry; mismatched values degrade similarity. Valid values:

| Task type | Use case |
| --- | --- |
| `RETRIEVAL_QUERY` | Query side of a search index. |
| `RETRIEVAL_DOCUMENT` | Document side of a search index. |
| `SEMANTIC_SIMILARITY` | Plain text-to-text similarity. |
| `CLASSIFICATION` | Inputs to a downstream classifier. |
| `CLUSTERING` | Unsupervised clustering. |
| `QUESTION_ANSWERING` | Q&A style retrieval. |
| `FACT_VERIFICATION` | Fact-vs-evidence comparison. |
| `CODE_RETRIEVAL_QUERY` | Code-search queries. |

### Implementation

`_embed` instantiates `langchain_google_genai.GoogleGenerativeAIEmbeddings(model=..., google_api_key=..., task_type=..., output_dimensionality=...)`. The `model` is prefixed with `models/` if it does not already start with one. A `reasoning_step` is emitted just before the call (`model, task_type, n`). The actual embed runs via `embedder.aembed_documents(texts)`.

## Config

`[agent.tool_source_config.gemini_embeddings]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `api_key` | string | `None` | Falls back to `GOOGLE_API_KEY` / `GEMINI_API_KEY` in `ctx.api_keys`. |
| `default_model` | string | `"gemini-embedding-001"` | Auto-prefixed with `models/`. |
| `default_task_type` | string | `None` | Validated against `_VALID_TASK_TYPES`. |
| `default_output_dimensionality` | int | `None` | Must be `>= 1` when set. |

Requires the `[google_genai]` extra (`langchain-google-genai` package).

## Related

- [`gemini_image` tool source](gemini_image.md) — sibling Gemini surface.
- [`rerank` tool source](rerank.md) — pair embeddings with a reranker for hybrid retrieval.
- [Operating: configuration](../../../operating/configuration.md).
