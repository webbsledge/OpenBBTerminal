# `openbb_agent_server.plugins.checkpointers.postgres`

Persistent Postgres-backed checkpointer using `langgraph.checkpoint.postgres.aio.AsyncPostgresSaver`. The provider owns the async connection-pool context manager and tears it down cleanly on shutdown; schema bootstrap runs on every `open()` via `saver.setup()`, so a fresh database is initialised on first start without an out-of-band migration step.

**Source:** [`openbb_agent_server/plugins/checkpointers/postgres.py`](../../../../openbb_agent_server/plugins/checkpointers/postgres.py)

## Classes

### `PostgresCheckpointerProvider`

Plugin entry-point name: `postgres`. Not the default — set `checkpointer_provider = "postgres"` (or `OPENBB_AGENT_CHECKPOINTER_PROVIDER=postgres`) to use.

#### Constructor

| Kwarg | Type | Default | Effect |
| --- | --- | --- | --- |
| `url` | `str \| None` | unset | The Postgres connection URL. When unset, falls back (in priority order) to `OPENBB_AGENT_CHECKPOINTER_URL`, then `settings.resolved_db_url()` (so by default the checkpointer rides on the same Postgres instance as the rest of persistence). |

Additional `**_config` kwargs are accepted and discarded so a stray `[checkpointer_config]` block doesn't break startup.

#### URL handling — `_resolve_url` and `_normalise_pg_url`

`_normalise_pg_url` strips SQLAlchemy-style driver suffixes that `psycopg` can't parse:

- `postgresql+psycopg://...` → `postgresql://...`
- `postgresql+asyncpg://...` → `postgresql://...`

After normalisation, the URL must start with `postgresql://`. Any other scheme (`sqlite://`, `mysql://`, …) raises `RuntimeError(f"Postgres checkpointer expects a postgresql:// URL, got {url!r}")` — fail-fast at startup rather than at first checkpoint write.

#### `async open(settings) -> AsyncPostgresSaver`

1. Imports `langgraph.checkpoint.postgres.aio.AsyncPostgresSaver` lazily. The package ships as an optional dependency; if missing, raises `RuntimeError("Postgres checkpointer requires langgraph-checkpoint-postgres and psycopg[binary]. Install the agent_server with the [postgres] extra.")` with the exact remediation.
2. Resolves the URL via `_resolve_url(settings)` (kwarg → env → `settings.resolved_db_url()` + normalisation).
3. `self._cm = AsyncPostgresSaver.from_conn_string(url)` — captures the **context manager** (not the saver). Holding the CM is what keeps the underlying `psycopg` async pool alive.
4. `saver = await self._cm.__aenter__()` — opens the pool.
5. `await saver.setup()` — idempotent schema bootstrap; safe to call on every start.
6. Logs `INFO postgres checkpointer opened against <url>` and returns the saver.

#### `async close(saver) -> None`

Calls `self._cm.__aexit__(None, None, None)` so `psycopg` closes the pool cleanly. `self._cm` is then set to `None` so a second `close()` is a no-op. The `saver` argument is unused — the lifetime is bound to the CM, not to the saver instance.

## Behaviour notes

- The provider does **not** validate that the configured DB exists; that surfaces on the first connection inside `__aenter__`. A typo in the URL therefore fails at server start, not at first checkpoint.
- `saver.setup()` is safe to call on a populated database — LangGraph's schema bootstrap is idempotent.
- The two `# pragma: no cover — needs live Postgres` markers on `open()` and `close()` exclude these branches from coverage; they're exercised by the operating-mode integration tests that spin up a real Postgres.

## TOML config example

```toml
[checkpointer]
provider = "postgres"

[checkpointer_config]
# Optional — defaults to OPENBB_AGENT_CHECKPOINTER_URL, then settings.resolved_db_url().
url = "postgresql://agent:secret@db.internal:5432/agent_server"
```

The agent server's `pyproject.toml` ships a `[postgres]` extra:

```bash
pip install "openbb-agent-server[postgres]"
```

which pulls in `langgraph-checkpoint-postgres` and `psycopg[binary]`.

## See also

- [`sqlite`](sqlite.md) — single-host persistent option.
- [`inmemory`](inmemory.md) — non-persistent test counterpart.
- [`../../runtime/plugins.md`](../../runtime/plugins.md) — the `CheckpointerProvider` plugin protocol.
- [`../../../operating/persistence.md`](../../../operating/persistence.md) — `settings.resolved_db_url()` and the rest of the persistence layer.
