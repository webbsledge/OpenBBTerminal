# `openbb_agent_server.plugins.subagents`

Built-in subagent specifications. Each module exports a deepagents-compatible class with `name`, `description`, `system_prompt`, `tools`, and `model` class attributes; the parent agent's planner uses these to decide when to delegate.

**Source:** [`openbb_agent_server/plugins/subagents/__init__.py`](../../../../openbb_agent_server/plugins/subagents/__init__.py)

| Subagent | Purpose |
| --- | --- |
| [`researcher`](researcher.md) | Web / news / filings retrieval with citation on every claim. Inherits the parent's full toolset. |
| [`analyst`](analyst.md) | Computes descriptive stats / group-bys / deltas over a dataset reference and emits one table artifact. |
| [`charter`](charter.md) | Turns a dataset reference into a single Plotly chart artifact (line / bar / scatter, chosen from data shape). |
| [`pdf_reader`](pdf_reader.md) | Extracts text + per-phrase bounding boxes from attached PDFs and emits anchored citations. Pinned to the `pdf_extract` tool. |

All four subagents share a common shape:

- Built-in entry-point classes; profile authors can replace or extend them through the standard plugin discovery (`pyproject.toml` `[project.entry-points."openbb_agent_server.subagents"]`).
- An empty `tools` tuple means *inherit the parent's tools*; a non-empty tuple narrows the surface (only `pdf_reader` does this).
- `model = None` means *inherit the parent's model*.

## Related

- [Writing a subagent](../../../developing/writing-a-subagent.md) — the contract these classes implement.
- [Plugin system](../../../developing/plugin-system.md) — entry-point discovery rules.
- [`tools/` reference](../tools/index.md) — the tool surfaces these subagents typically inherit.
