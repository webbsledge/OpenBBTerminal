# `openbb_agent_server.runtime.widget_store`

SQL-backed widget-data store with optional ANN row search via `sqlite-vec`. Every `get_widget_data` response observed in a tool message is normalised by `parse_widget_data_messages` and persisted here so subsequent turns (and the agent's `read_widget_data` / `query_widget_data` / `search_widget_data` tools) can read it without re-fetching.

**Source:** [`openbb_agent_server/runtime/widget_store.py`](../../../openbb_agent_server/runtime/widget_store.py)

## `class WidgetDataStore`

```python
WidgetDataStore(
    url: str,
    *,
    embeddings: Embeddings | None = None,
    engine: AsyncEngine | None = None,
)
```

| Arg | Purpose |
| --- | --- |
| `url` | SQLAlchemy async URL — typically the same as `HistoryStore` so both stores share one file. |
| `embeddings` | Optional `Embeddings`. When set, every row is indexed into a `widget_rows_vec` ANN table on top of the parent `widget_data` row. |
| `engine` | Pre-built `AsyncEngine`. Pass to share the engine across stores instead of constructing a second one. |

For SQLite URLs, the constructor applies WAL + `busy_timeout` pragmas — same as `SqliteHistoryStore`.

### `async def record(...) -> int`

Persist one ingestion. Two-phase:

1. **Synchronous** — commit the canonical `widget_data` row through the async engine. The id is returned to the caller before the vector index has been written, so later `read_widget_data` / `query_widget_data` calls see the row immediately.
2. **Fire-and-forget** — schedule `_index_in_background` on the running loop. The background task runs `_index_rows_sync` (one batch of 256 rows at a time, holding a `threading.Lock`) so a 10K-row widget doesn't stall the request and the ANN writer doesn't contend with the async engine.

When no running loop is available (test path), the indexing runs inline.

### `async def list_entries(*, principal, conversation_id=None) -> list[dict]`

List widgets ingested for this user. `conversation_id=None` spans every conversation the user owns — that's the agent's discovery path for data fetched in prior threads. Returns lightweight metadata only (`row_count`, no row payload).

### `async def read_latest(*, principal, conversation_id=None, widget_uuid=None, widget_name=None, max_rows=None) -> dict | None`

Return the most recent ingest matching `widget_uuid` OR `widget_name`. If a row is found by `widget_uuid` but the `widget_name` disagrees, falls through to a name-only search over the same scope. Returns `None` for no match.

### `async def search(*, principal, conversation_id=None, query, k=8, widget_uuid=None) -> list[dict]`

Top-`k` rows across the user's widget data.

| Path | When | Behaviour |
| --- | --- | --- |
| ANN (`_ann_search`) | `embeddings` configured AND vec store created | `similarity_search_with_score` over `widget_rows_vec`, then filter by `user_id` / `conversation_id` / `widget_uuid` and dedup by `(parent_id, row_idx)`. Score is `1 / (1 + distance)`. |
| Substring fallback (`_substring_search`) | No embeddings OR ANN returned no hits | Linear scan of every row's pipe-joined `key: value` text. Score is `1.0`. |

Each hit is `{"score": float, "row": dict, "widget_uuid": str, "widget_name": str | None}`.

### `async def schema(*, principal, conversation_id=None) -> list[dict]`

Return the table-name → columns mapping the agent should target for SQL. One row per widget, deduplicated by slug — the LATEST ingest per slug wins (so the table name is stable across turns when a widget is re-fetched).

### `async def query(*, principal, conversation_id=None, sql, max_rows=500) -> dict`

Run a READ-ONLY `SELECT` / `WITH` against the widget data. SQLite-only — Postgres dialect raises `RuntimeError`. The implementation:

1. Validates the SQL is a single `SELECT` / `WITH` (no DDL, no DML, no `PRAGMA`).
2. Loads every matching ingest in oldest-first order; dedup by slug keeps the latest per widget.
3. For each surviving ingest, creates a `CREATE TEMP VIEW "<slug>" AS SELECT json_extract(je.value, '$."col"') AS "col", … FROM widget_data wd, json_each(wd.rows) AS je WHERE wd.id = <ing_id>` — exposing the row payload as a virtual table.
4. Executes the query against the temp views, fetches `max_rows + 1` (to detect truncation), and returns `{"columns": [...], "rows": [...], "table_count": N, "truncated": bool}`.

The view alias names come from `_slugify_table_name(widget_name | widget_uuid)`; column names come from `ing.columns` when present, else the union of keys across `ing.rows`.

### `async def await_pending_indexing()`

Wait for every in-flight background indexing task to finish. Tests call this to make assertions about `record` side-effects deterministic.

## `def parse_widget_data_messages(body_messages) -> list[dict]`

Walk the wire-protocol messages and pull out `get_widget_data` results. Supports both:

1. **Structured AI envelope.** `role:"ai"` with top-level `function` / `input_arguments` fields.
2. **Legacy JSON-in-content.** `role:"ai"` with `content = '{"function": "get_widget_data", ...}'`.

For each AI envelope, looks at the following `role:"tool"` message and pairs its `data` payload with the envelope's `data_sources`. Tool-message-only payloads (no preceding AI envelope) are accepted as a fallback. When `data_sources` is missing entirely, falls back to `widget_ids` and synthesises minimal data sources so rows still get stored.

## Module helpers

| Helper | Purpose |
| --- | --- |
| `_slugify_table_name(name)` | Map a widget name to a safe SQLite identifier. Empty → `widget`; leading digit → underscore-prefixed. |
| `_row_text(row)` | Flatten a row dict to a pipe-joined `key: value` string for embedding / substring search. |
| `_url_to_file(url)` | Parse `sqlite[+driver]:///<path>` → file path (or `None` for `:memory:`). |
| `_build_vec_connection(file)` | Open a thread-safe `sqlite3` connection with WAL + `sqlite-vec` loaded. |
| `_extract_rows(payload)` / `_extract_columns(rows)` | Best-effort pull of `[{...}]` row dicts out of a Workspace data envelope (list / dict / JSON string). |

## See also

- [`memory/retrievers.md`](../memory/retrievers.md) — `WidgetDataRetriever` over `.search()`.
- [`persistence/models.md`](../persistence/models.md) — the `widget_data` table.
- [`plugins/tools/widget_data.md`](../plugins/tools/widget_data.md) — agent-callable surface.
- [`guides/widgets-and-data.md`](../../guides/widgets-and-data.md) — end-to-end flow.
