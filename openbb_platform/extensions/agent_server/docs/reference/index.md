# API Reference

One markdown file per Python module in `openbb_agent_server/`. Layout mirrors the package tree exactly. Module-level docstrings, public classes (with method signatures), and public functions (with one-line summaries) are surfaced here; for full bodies follow the `Source:` link to the `.py` file.

## Top-level

- [`main`](main.md) — CLI entry point.

## [`app/`](app/index.md) — FastAPI surface

- [`app.app`](app/app.md) — app factory + lifespan.
- [`app.router`](app/router.md) — `/agents.json`, `/v1/query`, `/v1/memory`, `/v1/conversations`, `/v1/traces`, `/v1/usage`, `/v1/me`.
- [`app.settings`](app/settings.md) — `AgentServerSettings`, `AgentProfile`, `AgentMetadata`.
- [`app.config`](app/config.md) — settings loaders (`SystemService` / env / `user_settings.json` / `openbb.toml`).

## [`runtime/`](runtime/index.md) — agent execution

- [`runtime.builder`](runtime/builder.md) — `run_agent()` composes plugins and yields SSE events.
- [`runtime.context`](runtime/context.md) — `RunContext`, `FileRef`, `WidgetRef`, `bind()` contextvar manager.
- [`runtime.identity`](runtime/identity.md) — `hash_user_id`, `is_email`, `redact_email_in_text` (PII helpers).
- [`runtime.principal`](runtime/principal.md) — `UserPrincipal`.
- [`runtime.jobs`](runtime/jobs.md) — `JobRegistry` for background tool execution.
- [`runtime.pdf_store`](runtime/pdf_store.md) — `PdfStore` (PDF ingestion, page extraction, search index).
- [`runtime.plugins`](runtime/plugins.md) — Plugin ABCs (`AuthBackend`, `ModelProvider`, `ToolSource`, `Middleware`, `SubAgentSpec`, `CheckpointerProvider`).
- [`runtime.registry`](runtime/registry.md) — entry-point discovery.
- [`runtime.services`](runtime/services.md) — globally-bound `HistoryStore` / `MemoryStore` / `WidgetDataStore` / `PdfStore` / checkpointer.
- [`runtime.widget_store`](runtime/widget_store.md) — `WidgetDataStore` (SQLAlchemy rows + `SQLiteVec` ANN index).
- [`runtime.emit`](runtime/emit.md) — `reasoning_step` / `html_artifact` / `markdown_artifact` / `table_artifact` / `chart_artifact` / `cite` / `function_call`.

## [`protocol/`](protocol/index.md) — wire types

- [`protocol.schemas`](protocol/schemas.md) — Pydantic models (`QueryRequest`, `MessageChunkSSE`, `StatusUpdateSSE`, `FunctionCallSSE`, `MessageArtifactSSE`, `CitationsSSE`).
- [`protocol.adapter`](protocol/adapter.md) — translate DeepAgents stream events into OpenBB SSE.
- [`protocol.sse`](protocol/sse.md) — SSE encoding helpers.

## [`memory/`](memory/index.md) — vector memory + retrieval

- [`memory.store`](memory/store.md) — `MemoryStore` ABC + `Memory` row.
- [`memory.sqlite_store`](memory/sqlite_store.md) — `SqliteMemoryStore` over `langchain_community.vectorstores.SQLiteVec`.
- [`memory.embeddings`](memory/embeddings.md) — `HashEmbeddings` fallback + `cosine` helper.
- [`memory.factory`](memory/factory.md) — `make_embeddings` / `make_reranker` / `make_translator`.
- [`memory.reranker`](memory/reranker.md) — `NvidiaReranker` (cross-encoder).
- [`memory.translation`](memory/translation.md) — `NvidiaTranslator` (Riva).
- [`memory.classifier`](memory/classifier.md) — `looks_like_code` heuristic for routing to the code embedder.
- [`memory.ingestion`](memory/ingestion.md) — `ingest_request_context` + LangChain document loaders + `RecursiveCharacterTextSplitter`.
- [`memory.retrievers`](memory/retrievers.md) — `MemoryStoreRetriever` + `WidgetDataRetriever` (LangChain `BaseRetriever` adapters).
- [`memory.writer`](memory/writer.md) — `MemoryWriter` post-turn extraction middleware.

## [`persistence/`](persistence/index.md) — chat history / usage / artifacts

- [`persistence.store`](persistence/store.md) — `HistoryStore` ABC + record types.
- [`persistence.sqlite_store`](persistence/sqlite_store.md) — SQLite/Postgres implementation.
- [`persistence.models`](persistence/models.md) — SQLAlchemy ORM tables.

## [`plugins/`](plugins/index.md) — built-ins

- [`plugins.auth`](plugins/auth/index.md) — `none`, `bearer_static`, `api_key_table`, `oidc_jwt`, `openbb_workspace`.
- [`plugins.models`](plugins/models/index.md) — chat-model providers.
- [`plugins.middleware`](plugins/middleware/index.md) — `call_limit`, `tool_call_limit`, `tool_call_announcer`, `tool_call_ledger`, `tool_filter`, `tool_message_normaliser`, `loop_guard`, `usage_recorder`.
- [`plugins.subagents`](plugins/subagents/index.md) — researcher, analyst, charter, pdf-reader.
- [`plugins.tools`](plugins/tools/index.md) — every shipped tool source.

## [`observability/`](observability/index.md)

- [`observability.logging`](observability/logging.md) — trace-aware structured JSON logger.

## [`prompts/`](prompts/index.md)

- [`prompts`](prompts/index.md) — packaged default system prompts.
