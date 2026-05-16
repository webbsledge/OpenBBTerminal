# `openbb_agent_server.plugins.tools.snowflake_tools.safety`

SQL safety guard: statement classification, read-only enforcement, and `LIMIT` auto-injection. Called from inside [`SnowflakeClient.execute`](client.md) before any agent-supplied SQL hits the connector. Parser is `sqlglot` with the `snowflake` dialect.

**Source:** [`openbb_agent_server/plugins/tools/snowflake_tools/safety.py`](../../../../../openbb_agent_server/plugins/tools/snowflake_tools/safety.py)

## Exceptions

| Class | When raised |
| --- | --- |
| `SnowflakeSafetyViolation` | `RuntimeError` subclass — raised by `enforce_read_only` when at least one parsed statement is mutating. |

## Read-only / mutating sets

`READ_ONLY_TYPES = {Select, Show, Describe, With, Union, Intersect, Except, Use}` — `USE WAREHOUSE/DATABASE/SCHEMA/ROLE` is allowed because it only mutates session state.

`MUTATING_TYPES = {Insert, Update, Delete, Create, Drop, Alter, AlterColumn, Merge, TruncateTable, Copy}`. Anything that matches one of these is rejected even if it co-occurs with read-only siblings in a multi-statement script.

For `exp.Command` nodes (which sqlglot returns for statements it doesn't model deeply), only the commands `EXPLAIN`, `USE`, `CALL`, `SHOW`, `DESCRIBE`, `DESC` are accepted as read-only.

## Functions

### `def parse(sql) -> list[exp.Expression]`

Normalise `%(name)s` → `:name` and `%s` → `?` (pyformat → sqlglot-friendly), then `sqlglot.parse(normalised, read="snowflake")`. Returns the list of non-None top-level statements. Multiple statements separated by `;` are supported.

### `def is_read_only(sql) -> bool`

Parse and check each statement. Returns `False` early on the first mutating statement; returns `False` if `parse` yields zero statements; returns `True` only if **every** statement is read-only.

### `def classify(sql) -> str`

Return a short label for the **first** statement:

- `"EMPTY"` if parsing yielded no statements.
- For `Command` nodes: the uppercased command name (or `"COMMAND"` if no name is set).
- Otherwise the class name of the AST node, uppercased (e.g. `"SELECT"`, `"WITH"`, `"INSERT"`).

This is what populates `QueryResult.statement_kind`.

### `def enforce_read_only(sql) -> None`

Raise `SnowflakeSafetyViolation(f"refused to run mutating statement: {classify(sql)}")` if `is_read_only(sql)` is False. No-op otherwise. Called from inside `SnowflakeClient.execute` whenever `self.read_only` is true.

### `def inject_limit(sql, max_rows) -> str`

Append `LIMIT max_rows` to the **single** top-level statement when:

- `max_rows > 0`.
- The SQL has no pyformat placeholders (parsed values would interfere with rewriting).
- There is exactly one parsed statement.
- The statement is `Select` / `Union` / `With`.
- No `LIMIT` is already present.

Otherwise returns `sql` unchanged. Uses `stmt.copy().set("limit", exp.Limit(expression=exp.Literal.number(max_rows)))` and emits with `dialect="snowflake"`.

## Related

- [`snowflake_tools.client`](client.md) — `SnowflakeClient.execute` is the only caller.
- [`snowflake_tools.source`](source.md) — every tool ultimately runs through `enforce_read_only` + `inject_limit`.
