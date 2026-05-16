# `openbb_agent_server.plugins.tools.snowflake_tools.source`

Wraps the [`SnowflakeClient`](client.md) and [Cortex helpers](cortex.md) into one plugin entry-point. Twenty `StructuredTool`s in total — query / catalog / schema introspection plus the full Cortex SQL surface (complete / summarise / sentiment / translate / classify / extract / embed) and the REST endpoints (search / analyst).

**Source:** [`openbb_agent_server/plugins/tools/snowflake_tools/source.py`](../../../../../openbb_agent_server/plugins/tools/snowflake_tools/source.py)

## Classes

### `SnowflakeToolSource`

Plugin entry-point name: `snowflake`. Constructor takes `credentials: dict | None`, `read_only: bool = True`, `max_rows: int = 10_000`, `connection_factory` (for tests). `tools(ctx, config)` layers credentials via `_credentials_from_ctx` (ctx.api_keys > plugin base > `SNOWFLAKE_*` env vars), constructs a `SnowflakeClient`, and hands off to `_build_tools`.

Also exposes `build_tools(client, creds, *, max_rows)` as a `@staticmethod` so other plugins that compose Snowflake can build the tool list directly without going through the source.

### Tools

| Tool | Args | Returns |
| --- | --- | --- |
| `snowflake_query` | `sql: str` (read-only SELECT/WITH/SHOW), `max_rows: int = <configured>` (∈ [1, 100_000]) | `{columns, rows, row_count, truncated, query_id, elapsed_ms, statement_kind}`. Non-empty results auto-emit a table artifact. Mutating SQL is rejected by [safety](safety.md). |
| `snowflake_list_databases` | — | Same shape (`SHOW DATABASES`). |
| `snowflake_list_schemas` | `database: str` | `SHOW SCHEMAS IN DATABASE <db>`. |
| `snowflake_list_tables` | `database: str`, `schema: str` (note: `protected_namespaces=()` so `schema` isn't shadowed by `BaseModel`) | `SHOW TABLES IN SCHEMA <db>.<schema>`. |
| `snowflake_describe` | `object_path: str` (e.g. `DB.SCHEMA.TABLE`) | `DESCRIBE <object_path>`. |
| `snowflake_get_table_info` | `table: str` (fully-qualified `DB.SCHEMA.TABLE`) | Richer column metadata from `INFORMATION_SCHEMA.COLUMNS` (`ordinal_position, column_name, data_type, is_nullable, column_default, comment`). Validates the dotted shape; raises `ValueError` otherwise. |
| `snowflake_get_table_sample_data` | `table: str`, `limit: int = 5` (∈ [1, 200]) | `SELECT * FROM <table>` capped at `limit`. |
| `snowflake_get_multiple_table_definitions` | `tables: list[str]` (1–50 fully-qualified names) | `{"tables": {<name>: <table_info or {error}>}}`. Per-table exceptions are captured as `{"error": "..."}` so a single bad name doesn't abort the batch. |
| `snowflake_search_catalog` | `pattern: str` (ILIKE with `%`), `limit: int = 50` (∈ [1, 500]) | Hits from `SNOWFLAKE.ACCOUNT_USAGE.COLUMNS` matching either `table_name` or `column_name`. |
| `snowflake_explain` | `sql: str` | `EXPLAIN <sql>`. |
| `snowflake_query_history` | `limit: int = 20` (∈ [1, 200]) | Recent rows from `TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())`. |
| `snowflake_cortex_complete` | `prompt: str`, `model: str = "claude-3-5-sonnet"`, `options: dict \| None` | Cortex chat completion text. |
| `snowflake_cortex_summarize` | `text: str` | Summary string. |
| `snowflake_cortex_sentiment` | `text: str` | Score `float` (-1..+1). |
| `snowflake_cortex_translate` | `text: str`, `target_language: str`, `source_language: str = ""` | Translated string. |
| `snowflake_cortex_classify` | `text: str`, `categories: list[str]` (≥ 2) | `{label, ...}` dict from `CLASSIFY_TEXT`. |
| `snowflake_cortex_extract_answer` | `question: str`, `context: str` | `{answer, score, ...}` dict from `EXTRACT_ANSWER`. |
| `snowflake_cortex_embed` | `text: str`, `model: str = "snowflake-arctic-embed-l-v2.0"`, `dim: int = 1024` (768 or 1024) | `list[float]` vector via `EMBED_TEXT_768` / `EMBED_TEXT_1024`. |
| `snowflake_cortex_search` | `database: str`, `schema: str`, `service: str`, `query: str`, `columns: list[str] \| None`, `limit: int = 10` (∈ [1, 200]), `filter: dict \| None` | Cortex Search REST response. Auto-cites each result. |
| `snowflake_cortex_analyst` | `messages: list[dict]` (OpenAI-style chat history), `semantic_model: str \| None` (stage path to YAML), `semantic_view: str \| None` (fully-qualified view) | Cortex Analyst REST response (text-to-SQL via the semantic layer). |

### Result rendering

`_result_to_payload(result)` is the shared post-processor:

- Non-empty results auto-emit a `table_artifact` via [`runtime/emit.py`](../../../runtime/emit.md) with description `<statement_kind> • query_id=<id> • <elapsed>ms` (plus `• truncated` when applicable).
- A `reasoning_step` (`event_type="SUCCESS"`) follows with the row count, elapsed time, query id, statement kind, and truncated flag.
- Returns `{columns, rows, row_count, truncated, query_id, elapsed_ms, statement_kind}` to the model.

### Credentials layering

`_credentials_from_ctx(ctx, base)` resolves credentials in this order (later wins):

1. Plugin constructor `credentials` dict (`SnowflakeCredentials.model_dump(by_alias=True, exclude_none=True)`).
2. `SNOWFLAKE_*` env vars (`SNOWFLAKE_ACCOUNT`, `_USER`, `_PASSWORD`, `_PRIVATE_KEY`, `_PRIVATE_KEY_PASSPHRASE`, `_AUTHENTICATOR`, `_TOKEN`, `_ROLE`, `_WAREHOUSE`, `_DATABASE`, `_SCHEMA`, `_HOST`, `_REGION`).
3. `ctx.api_keys` values for the same uppercase names.

The result is a single `SnowflakeCredentials` instance.

## Citation emission

`snowflake_cortex_search` automatically calls `emit.cite()` once per hit — `text` is the first 240 chars of `chunk` / `content`, `source` is the hit `title` or the service name, `source_url` is `url` / `source_url` if present.

## Side effects

- Every query emits a table artifact (non-empty results) and a reasoning step.
- The client's read-only enforcement runs through [safety.enforce_read_only](safety.md) before any execute call.
- `LIMIT max_rows` is auto-injected for plain SELECT / UNION / WITH statements when no LIMIT is already present and the SQL has no pyformat placeholders.

## Config

`[agent.tool_source_config.snowflake]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `read_only` | bool | `True` | Per-call override of the source's read-only flag. |
| `max_rows` | int | `10000` | Row cap; LIMIT is appended automatically when missing. |

Credentials are supplied via `ctx.api_keys` (per-request) or `SNOWFLAKE_*` env vars (per-process); not via the `[agent.tool_source_config.snowflake]` block.

Requires the `[snowflake]` extra (`snowflake-connector-python`).

## Related

- [`snowflake_tools.client`](client.md) — connection management.
- [`snowflake_tools.cortex`](cortex.md) — SQL + REST helpers.
- [`snowflake_tools.safety`](safety.md) — SQL guard.
- [`runtime/emit.py`](../../../runtime/emit.md) — citation / artifact wire shape.
