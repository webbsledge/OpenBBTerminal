# `openbb_agent_server.plugins.checkpointers.sqlite`

Persistent SQLite-backed checkpointer using `langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver`. The provider opens the underlying `aiosqlite.Connection` itself — rather than letting `AsyncSqliteSaver.from_conn_string()` open one with sqlite3 defaults — so the connection inherits the WAL / busy_timeout / synchronous=NORMAL pragmas that every other writer to the file relies on.

**Source:** [`openbb_agent_server/plugins/checkpointers/sqlite.py`](../../../../openbb_agent_server/plugins/checkpointers/sqlite.py)

## Classes

### `SqliteCheckpointerProvider`

Plugin entry-point name: `sqlite`. **Default** value of `AgentServerSettings.checkpointer_provider`.

#### Constructor

| Kwarg | Type | Default | Effect |
| --- | --- | --- | --- |
| `path` | `str \| None` | unset | Absolute or relative path to the checkpoint database file. When unset, falls back to (in priority order) `OPENBB_AGENT_CHECKPOINTER_PATH` env var, then `<settings.data_dir>/checkpoints.db`. |

Additional `**_config` kwargs are accepted and discarded so a stray `[checkpointer_config]` block doesn't break startup.

#### `async open(settings) -> AsyncSqliteSaver`

1. Resolves `path` (explicit kwarg → env var → `<settings.data_dir>/checkpoints.db`).
2. `asyncio.to_thread(Path(path).parent.mkdir, parents=True, exist_ok=True)` — `mkdir` is the one sync call in this startup path; off-loaded so the event loop isn't blocked on the filesystem.
3. `aiosqlite.connect(path, timeout=_BUSY_TIMEOUT_MS / 1000.0)` — opens **our** connection (not LangGraph's default) so we control the pragmas. The connection-level `timeout` is the `sqlite3.connect(timeout=...)` value (seconds); the explicit `PRAGMA busy_timeout` below pins it persistently.
4. Calls `_configure_connection(conn)` — runs the pragmas listed below.
5. Stashes the connection on `self._conn` and constructs `AsyncSqliteSaver(conn)`.
6. `await saver.setup()` — creates the LangGraph schema if it doesn't already exist.
7. Logs `INFO sqlite checkpointer opened at <path>` and returns the saver.

#### `async close(saver) -> None`

The saver doesn't own the connection — we do. `del saver` is purely cosmetic; the real shutdown is `await self._conn.close()`. `self._conn` is then set to `None` so a second `close()` is a no-op.

### Connection pragmas — `_configure_connection`

Applied on every connection the provider opens. `journal_mode=WAL` is file-level (persisted across processes) but harmless to re-issue; the rest are per-connection and **must** be set on every connection — including the one LangGraph uses for its own writes.

| Pragma | Value | Why |
| --- | --- | --- |
| `journal_mode` | `WAL` | Concurrent readers don't block the writer. Other stores in the codebase (history, widget, pdf) all rely on WAL on this file; setting it on every connection guarantees nobody falls back to rollback-journal. |
| `busy_timeout` | `30000` ms (`_BUSY_TIMEOUT_MS`) | Long enough to outlast a contention burst, short enough that a real deadlock surfaces instead of hanging forever. Matches the other stores, scaled up for the checkpointer's longer transactions. |
| `synchronous` | `NORMAL` | The WAL+NORMAL combo is safe across process crashes (only an OS crash can lose committed data). FULL is overkill for checkpoint data; OFF would risk corruption. |
| `temp_store` | `MEMORY` | Keeps temp B-trees out of the WAL and off the disk during longer queries. |

## Behaviour notes

- The provider opens **one** connection and keeps it for the agent's lifetime. AsyncSqliteSaver issues its writes through this connection, so the pragmas apply to every checkpoint write.
- `aiosqlite` runs the underlying `sqlite3.Connection` in its own worker thread, so the awaits don't block the event loop.
- The file path is `mkdir -p`'d on first open — a fresh deployment doesn't need to pre-create `data/`.

## TOML config example

```toml
[checkpointer]
provider = "sqlite"

[checkpointer_config]
# Optional — defaults to ${data_dir}/checkpoints.db
path = "/var/lib/openbb-agent/checkpoints.db"
```

## See also

- [`inmemory`](inmemory.md) — non-persistent test counterpart.
- [`postgres`](postgres.md) — multi-host production option.
- [`../../runtime/plugins.md`](../../runtime/plugins.md) — the `CheckpointerProvider` plugin protocol.
- [`../../../operating/persistence.md`](../../../operating/persistence.md) — how `data_dir` resolves and the SQLite file layout.
