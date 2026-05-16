# `openbb_agent_server.plugins.subagents.researcher`

Web / news / filings retrieval subagent. The parent agent delegates here when a question requires authoritative public sources (current events, regulatory filings, company announcements, primary documents) that the parent's tool belt does not cover — typically because the question depends on freshness rather than on the dashboard's pinned data. The researcher is expected to synthesise across multiple hits and emit citations on every factual claim it returns.

**Source:** [`openbb_agent_server/plugins/subagents/researcher.py`](../../../../openbb_agent_server/plugins/subagents/researcher.py)

## System prompt

Defined verbatim in `SYSTEM_PROMPT`. The instructions are deliberately short:

- Retrieve authoritative sources for the question (filings, primary documents, news).
- Summarise crisply.
- **Always** attach citations — `source_url` for URLs, or a named `source` — to every factual claim.
- If a tool fails or returns nothing, say so explicitly; do not invent.

The prompt is intentionally tool-agnostic. The researcher inherits whatever retrieval surface the parent has configured (typically [`web_search`](../tools/web_search.md) and the [`artifacts.cite_source`](../tools/artifacts.md) tool); the parent agent's planner decides which to invoke.

## Classes

### `ResearcherSubAgent`

The plugin entry-point class. Class attributes mirror the deepagents `SubAgent` shape:

| Attribute | Value |
| --- | --- |
| `name` | `"researcher"` |
| `description` | `"Use when the user asks for facts, sources, filings, news, or any answer that requires up-to-date retrieval and citation."` |
| `system_prompt` | `SYSTEM_PROMPT` (see above) |
| `tools` | `()` — inherits the parent's full tool set; no narrowing |
| `model` | `None` — uses the parent agent's model |

### `def factory(**_config)`

Legacy dict factory. Returns the same fields as the class — kept for backwards compatibility with profiles that reference a plain callable rather than the class. New profiles should reference the class directly.

## How to register

`pyproject.toml`:

```toml
[project.entry-points."openbb_agent_server.subagents"]
researcher = "openbb_agent_server.plugins.subagents.researcher:ResearcherSubAgent"
```

Profile binding (TOML):

```toml
[agent]
subagents = ["researcher", "analyst", "charter", "pdf_reader"]
```

## When the parent invokes this

Typical user prompts that route here:

- "What did Powell say in his speech yesterday?"
- "Find the latest 10-K language on segment X for $TICKER."
- "Has the SEC opened any enforcement actions against the company?"
- "Summarise this morning's news on the Fed dot plot."

The parent's planner detects the recency / external-source signal and hands off with the user's question intact. The researcher composes its own retrieval plan from the inherited tools.

## Related

- [`web_search` tool source](../tools/web_search.md) — typical primary retrieval surface.
- [`artifacts` tool source](../tools/artifacts.md) — provides `cite_source`.
- [`pdf_reader` subagent](pdf_reader.md) — preferred when the source is already attached as a PDF.
- [Writing a subagent](../../../developing/writing-a-subagent.md) — the contract this class implements.
