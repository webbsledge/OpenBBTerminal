# `openbb_agent_server.app.app`

FastAPI app factory plus the startup / shutdown lifespan that opens and tears down the shared services every plugin reaches for via `runtime.services`.

**Source:** [`openbb_agent_server/app/app.py`](../../../openbb_agent_server/app/app.py)

## Factory

### `def create_app(settings: AgentServerSettings | None = None) -> FastAPI`

Build the FastAPI app. When `settings` is `None`, the bootstrap launcher resolves a config from `OPENBB_AGENT_BOOTSTRAP_TOML` / the standard cascade.

Construction order:

1. **`settings.data_dir.mkdir(parents=True, exist_ok=True)`** — ensure the per-host data directory exists (default `~/.openbb_platform/agent`).
2. **`install_trace_logging()`** — install the structured JSON formatter (`observability/logging.py`) that carries `trace_id` / `run_id` / `conversation_id` / `user_id` on every line.
3. **Plugin selection.**
   - `_load_auth(settings)` resolves the `AuthBackend` entry point named in `settings.auth_backend`.
   - `_load_checkpointer_provider(settings)` resolves the `CheckpointerProvider` named in `settings.checkpointer_provider`.
4. **Storage construction.**
   - `SqliteHistoryStore(db_url)` for chat history / traces / usage / artifacts / citations.
   - `WidgetDataStore(db_url)` for fetched widget rows (SQL-queried).
   - `PdfStore(db_url, embeddings=...)` for PDF ingestion (see [`runtime/pdf_store.md`](../runtime/pdf_store.md)).
   - `SqliteMemoryStore(db_url, embeddings, code_embeddings, reranker, rerank_fanout)` for cross-thread vector memory.
5. **Memory pipeline.** `make_embeddings` / `make_reranker` / `make_translator` (from `memory/factory.py`) wrap NVIDIA NIM endpoints (or the configured provider) behind the LangChain `Embeddings` / `BaseRetriever` interfaces.
6. **`services.set_services(...)`** — bind `history`, `memory`, `widget_store`, `pdf_store` for the rest of the process.
7. **Lifespan.** `init_schema()` runs `Base.metadata.create_all` on the history DB; `checkpointer_provider.open(settings)` is awaited (sets up SQLite/Postgres tables for LangGraph state) and bound via `services.set_services(checkpointer=…)`. Shutdown awaits `checkpointer_provider.close()`, `history.aclose()`, and `services.reset()`.
8. **FastAPI app.** CORS allow-`*` (every origin / method / header, credentials on), then `build_router(...)` from [`router.py`](router.md) is mounted.

Returned app exposes the bound collaborators on `app.state` (`settings`, `auth`, `history`, `memory`, `checkpointer`) so test fixtures can introspect without poking the contextvar.

## Internal helpers

### `_load_auth(settings) -> AuthBackend`

Resolve the configured `AuthBackend` via `runtime.registry.load("openbb_agent_server.auth", settings.auth_backend, settings.auth_config)`.

### `_load_checkpointer_provider(settings) -> CheckpointerProvider`

Same shape for the checkpointer entry-point group. The provider's `open()` / `close()` are awaited inside the lifespan, NOT here.
