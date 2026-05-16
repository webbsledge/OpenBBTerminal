# Persistence

One SQLite (or Postgres) file holds two parallel layers:

| Layer | Storage |
| --- | --- |
| Chat history, traces, usage, artifacts, citations, widget data, PDF ingest, users, api keys | SQLAlchemy ORM (see schema below) |
| Cross-thread vector memory + per-row widget ANN index | `langchain_community.vectorstores.SQLiteVec` tables in the same file |

## URLs

Configure via `agent.db_url` / `OPENBB_AGENT_DB_URL`:

```
sqlite+aiosqlite:///<path/to/db.sqlite>       # default — $HOME/.openbb_platform/agent/history.db
postgresql+psycopg://user:pw@host:port/db     # multi-worker
sqlite+aiosqlite:///:memory:                  # tests only — vector memory tables are not shared
```

If `db_url` is unset, the server resolves it to `sqlite+aiosqlite:///$data_dir/history.db` where `$data_dir` defaults to `~/.openbb_platform/agent`.

## Schema bootstrap

`SqliteHistoryStore.init_schema()` runs `Base.metadata.create_all` at app startup. There is no separate migration tool today — schema changes ship with code and apply on next launch.

## Tables

Every table carries a `user_id` column and indexes it. Cross-user reads are rejected at the store API; SQL queries always include `WHERE user_id = ?`.

| Table | Primary key | Business key | Notable columns |
| --- | --- | --- | --- |
| `users` | `user_id` | — | `display_name`, `email`, `created_at`, `last_seen_at`, `quota_json`, `memory_opt_in` |
| `api_keys` | `key_id` | — | `user_id`, `hashed_secret`, `label`, `scopes`, `created_at`, `revoked_at` |
| `conversations` | `conversation_id` | — | `user_id`, `title`, `summary_blob_ref`, `created_at`, `updated_at` |
| `messages` | `id` (autoincrement) | `(user_id, conversation_id, seq)` | `role`, `content`, `widget_refs`, `file_refs`, `trace_id`, `ts` |
| `traces` | `trace_id` | — | `user_id`, `conversation_id`, `run_id`, `started_at`, `ended_at`, `status` |
| `runs` | `run_id` | — | `user_id`, `conversation_id`, `trace_id`, `model`, `system_prompt_hash`, `status` |
| `tool_calls` | `id` (autoincrement) | `(user_id, trace_id, seq)` | `tool_name`, `args_json`, `result_json`, `error`, `latency_ms`, `side`, `state` |
| `usage` | `id` (autoincrement) | `(user_id, trace_id, seq)` | `model`, `input_tokens`, `output_tokens`, `cache_read`, `cache_creation`, `cost_usd` |
| `artifacts` | `id` (autoincrement) | `(user_id, trace_id, seq)` | `kind`, `payload_blob_ref`, `payload_json`, `mime` |
| `citations` | `id` (autoincrement) | `(user_id, trace_id, seq)` | `source`, `source_url`, `page`, `bbox_json`, `text_snippet` |
| `pending_runs` | `run_id` | — | `user_id`, `state_blob` (checkpoint snapshot for resume) |
| `widget_data` | `id` (autoincrement) | — | `user_id`, `conversation_id`, `widget_uuid`, `widget_name`, `origin`, `input_args`, `columns`, `rows`, `ingested_at` |
| `pdf_documents` | `id` (autoincrement) | `(user_id, file_key)` | `name`, `url`, `mime`, `total_pages`, `metadata_json`, `toc_json`, `status`, `error`, `ingested_at` |
| `pdf_pages` | `id` (autoincrement) | `(pdf_id, page)` | `text`, `words_json` (FK → `pdf_documents.id`, cascade delete) |

`memory_opt_in` exists on `users` but is not currently read by any code path; memory writes are gated by the `memory:write` scope alone.

Vector tables (managed by SQLiteVec):

- `memories_text`, `memories_text_vec` — text-embedded memory rows.
- `memories_code`, `memories_code_vec` — created only when `embeddings_code_provider` is set.
- `widget_rows_vec` — per-row ANN index referencing `widget_data.id`.

Metadata on every vector row carries `user_id` so cross-user isolation is preserved at the SQL filter level.

## Read endpoints

| Endpoint | Returns |
| --- | --- |
| `GET /v1/conversations` | per-user list |
| `GET /v1/conversations/{conversation_id}/messages` | full message log for one thread |
| `GET /v1/traces/{trace_id}` | bundle: messages + tool_calls + usage + artifacts + citations |
| `GET /v1/usage` | aggregated usage; filter by `trace_id`, `conversation_id`, `from`, `to` |

## Postgres

Set `db_url` to a `postgresql+psycopg://...` URL. The ORM schema is identical; SQLiteVec tables still live in the same file under SQLite mode but are skipped under Postgres (`embeddings_provider` falls back to in-process hashing or whichever provider you select).

Cross-process cancellation is in-process only; `POST /v1/conversations/{id}/cancel` signals an `asyncio.Event` on the worker that's serving the run.

## Backups

- **SQLite:** copy the `.db` file (quiescent) or use `sqlite3 .backup`.
- **Postgres:** `pg_dump`.

## Source

- [`persistence/store.py`](../../openbb_agent_server/persistence/store.py)
- [`persistence/sqlite_store.py`](../../openbb_agent_server/persistence/sqlite_store.py)
- [`persistence/models.py`](../../openbb_agent_server/persistence/models.py)
- [`runtime/widget_store.py`](../../openbb_agent_server/runtime/widget_store.py)
- [`memory/sqlite_store.py`](../../openbb_agent_server/memory/sqlite_store.py)
