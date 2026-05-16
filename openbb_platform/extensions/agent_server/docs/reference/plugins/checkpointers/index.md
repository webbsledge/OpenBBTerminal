# `openbb_agent_server.plugins.checkpointers`

Built-in `CheckpointerProvider` implementations. The runtime selects exactly one via `AgentServerSettings.checkpointer_provider` — defaulting to `"sqlite"`. The selected provider's `open(settings)` is called at server start, the returned LangGraph saver is handed to the agent builder, and `close(saver)` runs at shutdown.

- [`inmemory`](inmemory.md) — process-local `langgraph.checkpoint.memory.InMemorySaver`. No persistence; every checkpoint lives in a Python dict and dies with the process. Used by the test suite.
- [`sqlite`](sqlite.md) — **default.** Persistent `AsyncSqliteSaver` over an `aiosqlite.Connection` the provider opens itself so the WAL + busy_timeout + synchronous=NORMAL pragmas every other writer to the file relies on are applied. Path defaults to `<settings.data_dir>/checkpoints.db`.
- [`postgres`](postgres.md) — production option. `AsyncPostgresSaver` over a `psycopg` async pool; URL falls back through `OPENBB_AGENT_CHECKPOINTER_URL` to `settings.resolved_db_url()`. Schema bootstrap runs on every `open()` via `saver.setup()`. Requires the `[postgres]` install extra.

**Source:** [`openbb_agent_server/plugins/checkpointers/__init__.py`](../../../../openbb_agent_server/plugins/checkpointers/__init__.py)

## See also

- [`../../runtime/plugins.md`](../../runtime/plugins.md) — the `CheckpointerProvider` plugin protocol.
- [`../../../operating/persistence.md`](../../../operating/persistence.md) — `data_dir`, `resolved_db_url()`, and the rest of the persistence layer.
- [`../../../developing/testing.md`](../../../developing/testing.md) — how the test harness picks `inmemory`.
