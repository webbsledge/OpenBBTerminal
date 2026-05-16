# `openbb_agent_server.plugins.tools.artifacts`

Server-side artifact + citation + reasoning-step emission. Bundles every "thing the agent shows the user" into one tool source so profiles get the full deliverable surface in a single bind. Calls flow through [`runtime/emit.py`](../../runtime/emit.md) and reach Workspace as SSE frames.

**Source:** [`openbb_agent_server/plugins/tools/artifacts.py`](../../../../openbb_agent_server/plugins/tools/artifacts.py)

## Classes

### `ArtifactsToolSource`

Plugin entry-point name: `artifacts`. `tools(ctx, config)` registers six `StructuredTool`s.

| Tool | Args | Returns |
| --- | --- | --- |
| `emit_html_artifact` | `content: str` (sanitised HTML), `name: str = ""`, `description: str = ""` | Success message including the artifact uuid; appends a sanitiser warning when scratchpad sections were stripped. The Workspace UI renders the HTML server-side after sanitisation (no scripts, no iframes). |
| `emit_markdown_artifact` | `content: str` (markdown body), `name: str = ""`, `description: str = ""` | Success message + uuid. The body is run through `_sanitise_markdown_body` first; any removals show up in the message so the model can self-correct. |
| `emit_table_artifact` | `columns: list` (column headers), `rows: list[list]` (cell rows), `name: str = ""`, `description: str = ""` | Success message + uuid. `columns` and `rows` accept JSON-encoded strings (the args validator decodes them) — covers chat models that stringify list args. |
| `emit_chart_artifact` | `plotly: dict` (Plotly figure JSON), `name: str = ""`, `description: str = ""` | Success message + uuid. `plotly` accepts a JSON-encoded string as well. |
| `emit_reasoning_step` | `message: str`, `event_type: str = "INFO"` (∈ INFO / WARNING / ERROR — SUCCESS is reserved for the run terminator) | `"OK. Reasoning step '<message>' shown to the user."` Emits one `StatusUpdateSSE` to the reasoning lane. |
| `cite_source` | `text: str \| None`, `source: str \| None`, `source_url: str \| None` (any subset) | `"OK. Citation queued for the end-of-turn flush."` Citations are buffered and emitted as one batch at end-of-turn so they decorate the final-answer bubble. |

The success message every artifact emitter returns is a STOP signal — it tells the model that the next response is the final 1–2 sentence textual takeaway and that no further artifact tools should be called unless the user explicitly asked for one artifact per dimension.

## Markdown sanitiser

`_sanitise_markdown_body` strips scratchpad content that the model habitually leaks into user-facing markdown. Two passes:

1. **Section-header strip.** Any heading (`# Foo`, `## Foo`, `**Foo**`, `**Foo**:`, or a bare line ending in `:`) whose label normalises to one of:

   `session intent`, `tool activity`, `tools used`, `tools called`, `steps taken`, `actions taken`, `methodology`, `method used`, `next steps`, `what i did`, `what we did`, `process followed`, `tool activity performed so far`, `sources`, `citations`, `references`, `source list`

   The heading and its body up to the next heading (or end of doc) are removed. The `sources` / `citations` / `references` strip is deliberate: Workspace renders real citations as clickable chips on every `cite_source` / PDF auto-citation, so inline source lists are duplicate noise and lose the click-to-navigate affordance.

2. **Inline tool-call strip.** Lines like `- pdf_extract(name='...', page_range=[..])` or `1. \`search_pdf(query=..., k=...)\` — extracted ...` are deleted via `_TOOL_CALL_LINE_RE`. The pattern covers every built-in tool name (PDF, widget, web, vision, audio, memory, artifact, emit_*).

3. **Whitespace collapse.** Consecutive blank lines are squashed to one and trailing whitespace is trimmed.

When anything was removed, `_sanitise_warning` is appended to the tool-result text so the model sees *why* its content changed and adjusts the next artifact.

`emit_html_artifact` reuses the same sanitiser — HTML headings still match the markdown patterns in practice (the heading regex covers `## Foo` style only, but the model rarely interleaves real `<h1>` tags).

## Citation emission

`cite_source` is the explicit citation tool. It calls [`emit.cite()`](../../runtime/emit.md), which buffers entries inside the per-run `RunContext` and flushes them as a single SSE frame at end-of-turn. Tools that auto-cite (`web_search`, `search_pdf`, `pdf_extract`, `query_widget_data`, `snowflake_cortex_search`) go through the same buffer.

## Config

`[agent.tool_source_config.artifacts]` is empty — the tools depend only on the active `RunContext` and the per-run emit channels.

## Related

- [`runtime/emit.py`](../../runtime/emit.md) — the wire shape for every artifact / reasoning / cite frame.
- [`charter` subagent](../subagents/charter.md), [`analyst` subagent](../subagents/analyst.md) — primary callers of `emit_chart_artifact` / `emit_table_artifact`.
- [Writing a tool source](../../../developing/writing-a-tool-source.md).
