# `openbb_agent_server.plugins.tools.inspect_widget_data`

Per-run inspection of widget data the agent has already fetched. Backed by the `widget_data` table; rows are queried with SQL and keyword-matched, not vector-indexed.

**Source:** [`openbb_agent_server/plugins/tools/inspect_widget_data.py`](../../../../openbb_agent_server/plugins/tools/inspect_widget_data.py)

## Classes

### `InspectWidgetDataToolSource`

Plugin entry-point name: `inspect_widget_data`. `tools(ctx, config)` registers five `StructuredTool`s — all READ-ONLY:

| Tool | Args | Returns |
| --- | --- | --- |
| `list_widget_data()` | — | `[{id, widget_uuid, widget_name, origin, input_args, columns, row_count, ingested_at}]` — every widget whose data has been fetched this conversation. Empty list means `get_widget_data` must be called first. |
| `read_widget_data(widget_uuid? / widget_name?)` | `widget_uuid: str` (preferred) or `widget_name: str` (the internal `widget_id` slug, not the display label) | `{widget_uuid, widget_name, origin, input_args, columns, rows, ingested_at}` or `None` if no match. |
| `search_widget_data(query, k=8)` | `query: str`, `k: int` | Top-k `[{score, row, widget_uuid, widget_name}]` sorted by relevance. Uses vector embeddings when configured; falls back to substring match. |
| `describe_widget_data()` | — | `[{table, widget_name, widget_uuid, columns, row_count}]` — the SQL surface for `query_widget_data`. All columns are TEXT in the underlying table; cast with `CAST("col" AS REAL)` for arithmetic. Call before `query_widget_data`. |
| `query_widget_data(sql)` | `sql: str` (READ-ONLY `SELECT` / `WITH` only) | `{columns, rows, table_count, truncated}`. Each ingested widget is exposed as a temp view; joins across widgets are supported. Citations referencing the queried widget tables are auto-emitted. |

### Citation emission

`query_widget_data` parses table references out of the SQL and auto-emits one `cite()` per referenced widget, anchoring back to the source widget chip in Workspace.

## Config

`[agent.tool_source_config.inspect_widget_data]` is currently empty.
