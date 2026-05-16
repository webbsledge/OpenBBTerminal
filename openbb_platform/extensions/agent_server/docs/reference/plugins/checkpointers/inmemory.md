# `openbb_agent_server.plugins.checkpointers.inmemory`

Process-local checkpointer. Wraps `langgraph.checkpoint.memory.InMemorySaver` — checkpoints live in a plain Python dict held by the saver instance and are lost when the process exits. Used by the test suite and ephemeral demos; never appropriate for a deployment that needs trace replay or cross-process resume.

**Source:** [`openbb_agent_server/plugins/checkpointers/inmemory.py`](../../../../openbb_agent_server/plugins/checkpointers/inmemory.py)

## Classes

### `InMemoryCheckpointerProvider`

Plugin entry-point name: `inmemory`. Not the default — `AgentServerSettings.checkpointer_provider` defaults to `"sqlite"`. Selectable via `checkpointer_provider = "inmemory"` (or `OPENBB_AGENT_CHECKPOINTER_PROVIDER=inmemory`).

#### Constructor

`__init__(**_config)` accepts and discards arbitrary kwargs — `InMemorySaver` takes no construction arguments we surface, but a `[checkpointer_config]` block shouldn't blow up the runtime if present.

#### `async open(settings) -> InMemorySaver`

Returns a fresh `InMemorySaver()`. The `settings` arg is unused — there is no on-disk path to resolve and no engine to dispose. Two consecutive `open()` calls on the same provider instance return two independent savers, each with its own empty checkpoint store.

#### `async close(saver) -> None`

No-op. There's no connection or file handle to release; the saver is garbage-collected when nothing else holds it.

## Behaviour notes

- **No persistence.** Restarting the process drops every checkpoint. A resumed trace immediately after a restart sees the agent start from scratch.
- **No cross-process visibility.** Workers in a multi-worker uvicorn setup each get their own saver; a trace started on worker A cannot be resumed on worker B. Pin to one worker, or use `sqlite` / `postgres`.
- **Memory-bounded by your process.** Long traces with large state will grow the saver dict without an LRU. Fine for tests, dangerous in production.

## TOML config example

```toml
[checkpointer]
provider = "inmemory"
# No config keys.
```

## See also

- [`sqlite`](sqlite.md) — the default; persistent on a single host.
- [`postgres`](postgres.md) — production multi-host option.
- [`../../runtime/plugins.md`](../../runtime/plugins.md) — the `CheckpointerProvider` plugin protocol.
- [`../../../developing/testing.md`](../../../developing/testing.md) — how the test harness picks `inmemory` automatically.
