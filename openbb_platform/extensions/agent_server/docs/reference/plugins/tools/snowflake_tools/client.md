# `openbb_agent_server.plugins.tools.snowflake_tools.client`

Snowflake connection management. Three classes:

- `SnowflakeCredentials` — pydantic model for connection inputs (account / user / auth method / role / warehouse / database / schema / host / region / timeouts).
- `QueryResult` — the immutable result of one executed statement.
- `SnowflakeClient` — a thin wrapper around the live connection that enforces read-only mode and auto-injects `LIMIT`.

Not a tool source — this is the connection layer that [`snowflake_tools.source`](source.md) builds tools on top of.

**Source:** [`openbb_agent_server/plugins/tools/snowflake_tools/client.py`](../../../../../openbb_agent_server/plugins/tools/snowflake_tools/client.py)

## Classes

### `SnowflakeCredentials`

Pydantic `BaseModel` with `extra="allow"` so unknown keys round-trip cleanly. Fields:

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `account` | str | `""` | Snowflake account locator. |
| `user` | str | `""` | Snowflake user. |
| `password` | str \| None | `None` | Username/password auth (avoid for production). |
| `private_key` | str \| None | `None` | PEM string **or** a path (auto-detected: no newline + contains `/`). |
| `private_key_passphrase` | str \| None | `None` | Passphrase for the PEM. |
| `authenticator` | str | `"snowflake"` | `"snowflake"` (password), `"oauth"`, `"programmatic_access_token"`, `"externalbrowser"`, etc. |
| `token` | str \| None | `None` | OAuth / PAT bearer token. |
| `role`, `warehouse`, `database`, `host`, `region` | str \| None | `None` | Standard connection knobs. |
| `schema_` | str \| None | `None` | Aliased to `"schema"` on input/output; the trailing `_` avoids shadowing `BaseModel.schema`. |
| `statement_timeout` | int | `60` | `STATEMENT_TIMEOUT_IN_SECONDS` applied at session open. |
| `network_timeout` | int | `60` | Passed straight to the connector. |

`to_connect_kwargs(self) -> dict[str, Any]` returns the kwargs `snowflake.connector.connect(**kwargs)` expects: only non-empty fields are forwarded, `schema_` is renamed to `schema`, `private_key` is materialised via `_loaded_private_key` (loaded with `cryptography.hazmat.primitives.serialization`, re-serialised as DER PKCS8 with no encryption — what the connector wants), and `client_session_keep_alive=False` is forced.

`_loaded_private_key(self) -> bytes` is path-tolerant: if the value has no newline but contains `/`, it's treated as a file path expanded with `Path(...).expanduser().read_text()`.

### `QueryResult`

Pydantic `BaseModel` (extra=`"allow"` so unrecognised metadata round-trips). Fields:

| Field | Type | Notes |
| --- | --- | --- |
| `sql` | str | The (possibly LIMIT-injected) statement that ran. |
| `statement_kind` | str | The classifier label (`SELECT`, `SHOW`, `DESCRIBE`, …). |
| `columns` | list[str] | Column names from cursor description. |
| `rows` | list[list[Any]] | Row tuples coerced to lists. |
| `row_count` | int | `len(rows)` after truncation. |
| `truncated` | bool | True iff the result was capped at `max_rows`. |
| `query_id` | str \| None | `cur.sfqid` / `cur.query_id` / a local-only `local-<hex>` fallback. |
| `elapsed_ms` | int \| None | Wall-clock duration in ms. |
| `warning` | str \| None | Optional warning string. |

### `SnowflakeClient`

Thin wrapper around the connector. Constructor:

```python
SnowflakeClient(
    credentials: SnowflakeCredentials,
    *,
    connection_factory: ConnectionFactory | None = None,  # for tests
    read_only: bool = True,
    max_rows: int = 10_000,
)
```

`ConnectionFactory = Callable[[SnowflakeCredentials], Any]` — defaults to `_default_connection_factory` which imports `snowflake.connector` and calls `snowflake.connector.connect(**creds.to_connect_kwargs())`. Missing snowflake extra → `RuntimeError` with an install hint.

#### Lifecycle

- `open()` — lazy connect, then `_apply_session_settings` runs `ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = <statement_timeout>` (errors swallowed at DEBUG log level).
- `close()` — close the connection if open; errors swallowed.
- `__enter__` / `__exit__` — context-manager support.

#### `execute(sql, params=None, *, max_rows=None) -> QueryResult`

1. If `self.read_only`, calls [`safety.enforce_read_only(sql)`](safety.md) — mutating statements raise `SnowflakeSafetyViolation`.
2. Caps rows via `safety.inject_limit(sql, cap)` (no-op when pyformat placeholders are present or the statement is not a SELECT/UNION/WITH or already has a LIMIT).
3. Auto-reconnects on session-expired exceptions (errno ∈ `SESSION_EXPIRED_CODES = {390111, 390112, 390114}` or any of those numbers in the error message). The retry happens once at cursor-open time and once at execute time.
4. Runs `cur.fetchmany(cap + 1)` (one extra row to detect truncation) and trims back to `cap`.
5. Returns `QueryResult` with elapsed wall time, query id (from `cur.sfqid` / `cur.query_id` / a `local-<hex>` fallback), and the truncated flag.

`statement_kind` comes from [`safety.classify`](safety.md) on the prepared SQL.

## Related

- [`snowflake_tools.source`](source.md) — uses this client to back every tool.
- [`snowflake_tools.safety`](safety.md) — the SQL guard called inside `execute`.
- [`snowflake_tools.cortex`](cortex.md) — uses the client for Cortex SQL helpers (REST helpers bypass the client and talk straight to the Snowflake REST API).
