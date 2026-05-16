# `openbb_agent_server.persistence`

Persistence layer: SQLAlchemy ORM, the `HistoryStore` ABC, and a single async implementation that targets either SQLite (default) or Postgres (via `db_url` override). Every per-user row is keyed by `user_id` and cascade-deleted by `DELETE /v1/me`.

**Source:** [`openbb_agent_server/persistence/__init__.py`](../../../openbb_agent_server/persistence/__init__.py)

## Pages

| Page | What it covers |
| --- | --- |
| [`store.md`](store.md) | `HistoryStore` ABC + the immutable record types (`TraceRecord`, `MessageRecord`, `ToolCallRecord`, `UsageRecord`). All methods are principal-scoped. |
| [`sqlite_store.md`](sqlite_store.md) | `SqliteHistoryStore` — the SQLAlchemy-async implementation. WAL pragmas, `init_schema` bootstrap, per-method query notes, and the cascade-delete order. |
| [`models.md`](models.md) | The full ORM schema: `users`, `api_keys`, `conversations`, `messages`, `traces`, `runs`, `tool_calls`, `usage`, `artifacts`, `citations`, `widget_data`, `pdf_documents`, `pdf_pages`, `pending_runs`. |

## Topology

Every persistent row is owned by one `users.user_id`. The cascade-delete order for `DELETE /v1/me` is:

1. `messages` (FK to `conversations`).
2. `tool_calls`, `usage`, `artifacts`, `citations` (FK-less but join-keyed on `trace_id` + `user_id`).
3. `pending_runs`, `runs` (per-run state).
4. `traces`.
5. `conversations`.
6. `api_keys`.
7. `users`.

`widget_data` and `pdf_documents` / `pdf_pages` cascade through FKs on `users.user_id` and `pdf_documents.id` respectively — they delete with the parent rather than being walked explicitly.

## Engine selection

| `db_url` shape | Driver | Notes |
| --- | --- | --- |
| Unset (default) | `sqlite+aiosqlite:///{data_dir}/history.db` | WAL + busy_timeout + synchronous=NORMAL pragmas applied on every connect. |
| `sqlite+aiosqlite://…` | aiosqlite | Same pragmas. |
| `postgresql+asyncpg://…` | asyncpg | Pragmas are skipped — Postgres handles concurrency natively. |

## See also

- [`operating/persistence.md`](../../operating/persistence.md) — operational guide, Postgres setup.
- [`runtime/widget_store.md`](../runtime/widget_store.md) — uses the `widget_data` table plus a `sqlite-vec` index.
- [`plugins/auth/api_key_table.md`](../plugins/auth/api_key_table.md) — uses the `api_keys` and `users` tables.
- [`plugins/checkpointers/index.md`](../plugins/checkpointers/index.md) — LangGraph checkpointer providers, sharing the DB URL.
