# `openbb_agent_server.app`

FastAPI app factory, the request router, and the layered settings + config loader that drive the server.

**Source:** [`openbb_agent_server/app/__init__.py`](../../../openbb_agent_server/app/__init__.py)

## Pages

| Page | What it covers |
| --- | --- |
| [`app.md`](app.md) | `create_app(settings)` — the FastAPI factory. Wires every plugin (auth, history, memory, translator, widget store, checkpointer) and mounts the router. |
| [`router.md`](router.md) | The `/agents.json` + `/v1/query` + per-user (`/v1/me`, `/v1/memory`, `/v1/traces`, `/v1/usage`, `/v1/conversations`) surface. Per-endpoint scopes and cross-cutting behaviour (cancellation, trace headers, widget pre-fetch, PDF promotion). |
| [`settings.md`](settings.md) | `AgentServerSettings`, `AgentMetadata`, `AgentProfile`, `DEFAULT_FEATURES`, `SEARCH_WEB_FEATURE`. Every field with default + env-var name. |
| [`config.md`](config.md) | Layered TOML bootstrap — `pyproject [tool.openbb]` → user-global → project → explicit → `[env]` → `${VAR}` expansion. |

## Boot order

1. **`main.py`** sniffs `--config-file` / env fallbacks out of argv (without argparse) and calls `bootstrap_launcher_config(...)` so `os.environ` is fully populated BEFORE any heavy import.
2. **`AgentServerSettings.from_toml(agent_cfg)`** resolves env-vs-TOML (env wins) and returns the frozen settings model.
3. **`create_app(settings)`** opens the persistence engine, runs `init_schema`, opens the memory store + widget store, instantiates the auth backend + checkpointer through `runtime.registry.load(...)`, and mounts the router.
4. **`uvicorn`** binds the resolved host / port.

## Shutdown order

1. **`/v1/conversations/{id}/cancel`** signals the per-run `asyncio.Event`; in-flight runs raise `CancelledError`.
2. **FastAPI shutdown handler** awaits each store's `aclose()` / `await_pending_indexing()` so the SQLite WAL flushes cleanly and no in-flight vector write is lost.
3. **`CheckpointerProvider.close(saver)`** tears down LangGraph connections / files.

## Collaborators threaded through the router

`build_router(*, settings, auth, history, memory, translator, widget_store)` takes every per-server collaborator as an explicit parameter so tests can substitute fakes and operators can override individual pieces without re-implementing the factory:

| Collaborator | Type | Source |
| --- | --- | --- |
| `settings` | `AgentServerSettings` | [`settings.md`](settings.md) |
| `auth` | `AuthBackend` | [`runtime/plugins.md`](../runtime/plugins.md) |
| `history` | `HistoryStore` | [`persistence/store.md`](../persistence/store.md) |
| `memory` | `MemoryStore` | [`memory/store.md`](../memory/store.md) |
| `translator` | `NvidiaTranslator \| None` | [`memory/translation.md`](../memory/translation.md) |
| `widget_store` | `WidgetDataStore` | [`runtime/widget_store.md`](../runtime/widget_store.md) |

## See also

- [`main.md`](../main.md) — CLI entry point that constructs the app.
- [`operating/configuration.md`](../../operating/configuration.md) — operator's guide.
- [`developing/plugin-system.md`](../../developing/plugin-system.md) — how the swappable pieces fit together.
- [`runtime/index.md`](../runtime/index.md) — per-request runtime layer the app wires into.
- [`protocol/index.md`](../protocol/index.md) — the wire layer the router streams through.
