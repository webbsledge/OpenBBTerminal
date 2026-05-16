# `openbb_agent_server.plugins.subagents.analyst`

Given a dataset reference — a widget id, a `read_file` path, or inline JSON — the analyst computes descriptive statistics, group-bys, deltas, or cross-tabs and returns them as a single table artifact. The parent agent reaches here whenever the user asks "what's the average / median / total / breakdown by X" of data the agent has already fetched.

**Source:** [`openbb_agent_server/plugins/subagents/analyst.py`](../../../../openbb_agent_server/plugins/subagents/analyst.py)

## System prompt

Defined verbatim in `SYSTEM_PROMPT`. The instructions tell the analyst to:

- Treat the input as a dataset reference and compute the statistic the user asked for.
- Emit exactly **one** table artifact via the `custom` stream channel:

  ```json
  {"type": "artifact", "artifact_type": "table", "title": "...",
   "data": {"columns": [...], "rows": [...]}}
  ```

- Keep the markdown reply terse — the numbers live in the table, not the chat bubble.

The artifact shape matches what [`emit_table_artifact`](../tools/artifacts.md) produces server-side, so analysts that prefer the helper tool can call it instead of constructing the JSON manually.

## Classes

### `AnalystSubAgent`

| Attribute | Value |
| --- | --- |
| `name` | `"analyst"` |
| `description` | `"Use when the user asks for descriptive statistics, group-bys, deltas, or any tabular numeric summary of a dataset reference."` |
| `system_prompt` | `SYSTEM_PROMPT` (see above) |
| `tools` | `()` — inherits the parent's tools |
| `model` | `None` — inherits the parent's model |

The empty `tools` tuple is intentional: the analyst expects the parent to expose data-access tools ([`widget_data`](../tools/widget_data.md), [`inspect_widget_data`](../tools/inspect_widget_data.md), `read_file`) and artifact emitters in scope. It does not pin its own narrower toolset.

## How to register

```toml
[project.entry-points."openbb_agent_server.subagents"]
analyst = "openbb_agent_server.plugins.subagents.analyst:AnalystSubAgent"
```

## When the parent invokes this

- "Give me the mean and std-dev of returns for these tickers."
- "Break down revenue by segment year-over-year."
- "Compute month-over-month deltas of this series."
- "Cross-tab holdings by sector and rating."

The parent typically calls `get_widget_data` / `read_file` first to materialise the dataset, then hands the reference to the analyst with the user's analytical question intact.

## Related

- [`charter` subagent](charter.md) — sibling that produces Plotly charts instead of tables.
- [`inspect_widget_data` tool source](../tools/inspect_widget_data.md) — the analyst's primary SQL-style data surface.
- [`artifacts` tool source](../tools/artifacts.md) — provides `emit_table_artifact`.
- [Writing a subagent](../../../developing/writing-a-subagent.md).
