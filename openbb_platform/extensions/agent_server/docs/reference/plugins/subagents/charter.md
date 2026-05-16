# `openbb_agent_server.plugins.subagents.charter`

Turns a dataset reference into a single Plotly chart artifact. The parent agent delegates here when the user asks for a "chart", "plot", "graph", or "visualisation" of data that has already been materialised (a widget, an uploaded spreadsheet, or a tool result).

**Source:** [`openbb_agent_server/plugins/subagents/charter.py`](../../../../openbb_agent_server/plugins/subagents/charter.py)

## System prompt

Defined verbatim in `SYSTEM_PROMPT`. The charter is instructed to:

- Take a dataset reference (widget id, file path, or pasted JSON) as input.
- Produce a single chart artifact via the `custom` stream channel:

  ```json
  {"type": "artifact", "artifact_type": "chart", "title": "...",
   "data": {"plotly": <plotly figure JSON>}}
  ```

- Pick the chart type from the data shape — **line** for time series, **bar** for categorical comparisons, **scatter** for two numeric axes.
- Keep titles short.
- Never inline the full data array into the chat reply; the array lives in the artifact only.

The artifact shape mirrors what [`emit_chart_artifact`](../tools/artifacts.md) produces; charters that prefer the helper tool can call it directly.

## Classes

### `CharterSubAgent`

| Attribute | Value |
| --- | --- |
| `name` | `"charter"` |
| `description` | `"Use when the user asks for a chart / visualisation / plot of data they've referenced (a widget, an uploaded spreadsheet, or a tool result)."` |
| `system_prompt` | `SYSTEM_PROMPT` (see above) |
| `tools` | `()` — inherits the parent's tools |
| `model` | `None` — inherits the parent's model |

The empty `tools` tuple keeps the charter's surface flexible — it expects the parent to expose data-access tools and `emit_chart_artifact` in scope.

## How to register

```toml
[project.entry-points."openbb_agent_server.subagents"]
charter = "openbb_agent_server.plugins.subagents.charter:CharterSubAgent"
```

## When the parent invokes this

- "Plot the revenue trend for these companies."
- "Show me a bar chart of holdings by sector."
- "Scatter EPS vs price-to-book for the watchlist."
- "Visualise the yield curve for today."

The parent typically materialises the dataset first (`get_widget_data`, `read_file`, `query_widget_data`) and then hands the reference to the charter together with the user's instruction.

## Related

- [`analyst` subagent](analyst.md) — sibling that returns a table instead of a chart.
- [`inspect_widget_data` tool source](../tools/inspect_widget_data.md) — the charter's primary data surface for already-fetched widgets.
- [`artifacts` tool source](../tools/artifacts.md) — provides `emit_chart_artifact`.
- [Writing a subagent](../../../developing/writing-a-subagent.md).
