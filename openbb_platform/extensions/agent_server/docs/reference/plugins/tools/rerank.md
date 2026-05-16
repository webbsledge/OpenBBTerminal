# `openbb_agent_server.plugins.tools.rerank`

Cross-encoder reranker over NVIDIA NIM. One tool — `rerank` — to score a list of candidate passages against a query and return the top-k by descending relevance. Designed to plug in after `web_search` / tool fan-out / subagent results to pick the most relevant documents before reading them in full.

**Source:** [`openbb_agent_server/plugins/tools/rerank.py`](../../../../openbb_agent_server/plugins/tools/rerank.py)

## Classes

### `NvidiaRerankToolSource`

Plugin entry-point name: `rerank`. Constructor takes `model`, `api_key`, `base_url`, `truncate` (head/end truncation when passages exceed the model context). `tools(ctx, config)` builds one `NvidiaReranker` per agent run (api key sourced from `ctx.api_keys["NVIDIA_API_KEY"]` → config → constructor) and registers a single `StructuredTool`.

| Tool | Args | Returns |
| --- | --- | --- |
| `rerank` | `query: str` (query to rank candidates against), `candidates: list[str]` (each entry is one candidate passage / search result / chunk; the validator decodes JSON-string lists from chat models that stringify list args), `top_k: int = 5` (∈ [1, 200]) | `list[{index, score, text}]` sorted by descending relevance. `index` points back into the input `candidates` list. |

### Argument coercion

`candidates` carries `BeforeValidator(_decode_if_string)` — when a chat model serialises the list as a JSON string (`'["a", "b"]'`), the validator decodes it back to a Python list before pydantic validates the element types. Non-JSON strings pass through unchanged so pydantic can surface the type error cleanly.

Each candidate is coerced to `str` and tagged with its integer index before being sent to the reranker — the index is what `_run` uses to map the model's `(rank_id, score)` tuples back to the original passages.

## Failure behaviour

If the underlying `NvidiaReranker.rerank` call raises, the tool logs a warning and returns a **structured fallback**: the first `top_k` candidates in input order, each with `score=0.0`. The agent can still proceed — it just doesn't gain a reranking signal that turn.

If `candidates` is empty, the tool returns `[]` immediately without calling the model.

## Config

`[agent.tool_source_config.rerank]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `model` | string | `"nv-rerank-qa-mistral-4b:1"` | Any NIM reranker. |
| `api_key` | string | `None` | Falls back to `ctx.api_keys["NVIDIA_API_KEY"]`. |
| `base_url` | string | `None` | Override the NIM endpoint. |
| `truncate` | string | `"END"` | Passage-truncation policy passed to the reranker; the alternative is `"START"`. |

## Related

- [`translate` tool source](translate.md) — sibling NIM specialist wrapper.
- [`web_search` tool source](web_search.md) — primary upstream source of candidates to rerank.
- [Operating: configuration](../../../operating/configuration.md).
