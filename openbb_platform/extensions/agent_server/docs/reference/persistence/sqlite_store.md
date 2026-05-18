# `openbb_agent_server.persistence.sqlite_store`

The single `HistoryStore` implementation. Talks to SQLite (default) or Postgres (override `db_url`) through SQLAlchemy's async engine. SQLite connections are upgraded to WAL with a generous `busy_timeout` so the sync `SQLiteVec` writers in `memory/` and `runtime/widget_store.py` don't deadlock against the async aiosqlite writer.

**Source:** [`openbb_agent_server/persistence/sqlite_store.py`](../../../openbb_agent_server/persistence/sqlite_store.py)

## `class SqliteHistoryStore(HistoryStore)`

```python
SqliteHistoryStore(url: str)
```

| Arg | Purpose |
| --- | --- |
| `url` | SQLAlchemy async URL — typically `sqlite+aiosqlite:///path/to/history.db` or `postgresql+asyncpg://…`. |

Construction is cheap — the engine is built but no schema work runs until `init_schema()` is awaited.

## Pragmas

For SQLite URLs, a `connect` listener applies three pragmas on every connection:

```sql
PRAGMA journal_mode  = WAL;       -- concurrent readers + queued writer
PRAGMA busy_timeout  = 5000;      -- 5s wait before "database is locked"
PRAGMA synchronous   = NORMAL;    -- fsync per checkpoint, not per write
```

WAL is essential because the SQLite memory store, the widget-row vector store, and the history store all write to the same file in a default install. Without WAL the async engine and the sync `sqlite3` connections will deadlock the moment they overlap.

For Postgres URLs the pragma block is a no-op.

## Lifecycle methods

| Method | Purpose |
| --- | --- |
| `async init_schema()` | Run `Base.metadata.create_all` against the engine. Zero-config bootstrap — call once during app startup. No migrations: schema changes are additive and managed by hand. |
| `async aclose()` | Dispose the engine. Called on FastAPI shutdown. |

## `HistoryStore` method implementations

All methods enforce `user_id` partitioning. Cross-user lookups fail closed without leaking existence ("conversation not found" / returns `None`).

| Method | Implementation notes |
| --- | --- |
| `upsert_user(principal)` | First-sight insert; subsequent calls refresh `display_name` / `email` and bump `last_seen_at`. |
| `delete_user(principal)` | Cascade-deletes from `messages`, `tool_calls`, `usage`, `artifacts`, `citations`, `pending_runs`, `runs`, `traces`, `conversations`, `api_keys`, then the `users` row itself — in that order to satisfy FK constraints. |
| `begin_trace` | Inserts the `traces` row at run start. On reuse of an existing `trace_id`, checks `user_id` matches and resets `ended_at` / `status` to `"running"`. Cross-user reuse raises `PermissionError`. |
| `end_trace` | Sets `ended_at = now()` and `status`. Silently no-ops on missing trace (idempotent). |
| `append_message` | Looks up the `Conversation` row; creates it on first use. Computes `seq` as `max(existing) + 1`. Cross-user conversation lookup raises `PermissionError` with a generic "conversation not found" message. Returns the new `seq`. |
| `list_conversations` | `SELECT … FROM conversations WHERE user_id = ? ORDER BY updated_at DESC LIMIT ?`. |
| `get_messages` | Per-conversation message log; filtered by `user_id` AND `conversation_id`, ordered ascending by `seq`. |
| `record_tool_call` | Allocates a per-trace `seq` via `_next_seq()` (`max + 1` under the partition), then inserts. |
| `record_usage` | Same `seq` pattern. Raises `PermissionError` if `usage.user_id` doesn't match the principal — guards against a misrouted call from a plugin. |
| `get_trace_bundle` | Multi-query join: trace row + tool_calls + usage + artifacts + citations + (optionally) messages bound to this trace. Returns `None` for cross-user trace_id. Returned dict is the wire shape of `GET /v1/traces/{id}`. |
| `usage_summary` | `GROUP BY model` aggregation. Optional `trace_id` / `conversation_id` filters. The `conversation_id` path joins against `traces` to translate. Returns `{"by_model": [...]}`. |

## Private helpers

| Helper | Purpose |
| --- | --- |
| `_scoped_get_trace(session, principal, trace_id)` | Fetch a trace row, return `None` on missing or wrong user. |
| `_next_seq(session, table, trace_id, principal)` | Allocate the next `seq` for one of the `(trace_id, user_id)`-keyed audit tables (`tool_calls`, `usage`, `artifacts`, `citations`). |
| `_cascade_delete(session, user_id)` | The fixed-order DELETE chain used by `delete_user`. |

## See also

- [`persistence/store.md`](store.md) — ABC and record types.
- [`persistence/models.md`](models.md) — the ORM schema this targets.
- [`operating/persistence.md`](../../operating/persistence.md) — Postgres setup notes.
