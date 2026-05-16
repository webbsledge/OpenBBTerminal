# `openbb_agent_server.plugins.tools.widget_data`

Read user-pinned dashboard widgets. Two tools: a cheap inventory (`list_widgets`) and a dispatcher (`get_widget_data`) that hands the actual fetch back to Workspace via an SSE function-call frame. The model never sees raw widget rows directly from this source — they arrive on the next turn after Workspace executes the fetch and posts the result.

**Source:** [`openbb_agent_server/plugins/tools/widget_data.py`](../../../../openbb_agent_server/plugins/tools/widget_data.py)

## Classes

### `WidgetDataToolSource`

Plugin entry-point name: `widget_data`. `tools(ctx, config)` registers two `StructuredTool`s.

| Tool | Args | Returns |
| --- | --- | --- |
| `list_widgets` | — | `{count, widgets: [{widget_id, params_hash, data_hash, name?, description?}]}`. The hashes are 8-byte blake2s digests of the stably-JSON-encoded params / data, so the model can detect changes between turns without seeing full payloads. Emits one `reasoning_step` with the count. |
| `get_widget_data` | `widget_ids: list[str]` (per-instance widget uuids from the attached-widgets snapshot; the validator coerces JSON-encoded strings and comma-separated strings into a real list) | `"Dispatched fetch for N widget(s) to Workspace. End this turn — no further tool calls, no reply text. The rows arrive on the next turn."` Internally emits one `FunctionCallSSE` with `tool_name="get_widget_data"` and `parameters={"data_sources": [...]}`. |

`get_widget_data` is marked `return_direct=True`. The moment LangGraph emits the `ToolMessage`, the agent loop hits END instead of re-prompting the model. This is the proper exit point for a client-side dispatch — the SSE frame is what Workspace consumes; the model has nothing more to do until the next user turn.

## The auto-fetch flow

1. Model calls `get_widget_data` with one or more `widget_ids`.
2. The tool resolves each id against `RunContext.widgets`; unknown ids raise `ValueError` with the available `[{uuid, widget_id}]` list embedded for debugging.
3. The tool builds a `data_sources` list: each entry carries `widget_uuid` (the per-instance uuid), `origin` (the backend hint), `id` (the widget slug — falls back to uuid), and `input_args` (a copy of the widget params).
4. `emit.function_call("get_widget_data", {"data_sources": [...]})` flushes one SSE frame.
5. The agent run ends. Workspace's UI sees the frame, runs the fetch, and posts the rows back as the tool result on the next turn.

The agent description tells the model: **CALL THIS ONCE PER TURN with every widget you need — do not loop calling it per id.** The batching is what keeps the round-trip count low.

## Security

`widget_ids` come from the model. The dispatcher validates every id against `RunContext.widgets` before constructing the SSE frame — the model cannot smuggle in arbitrary widget uuids. Params are copied from the trusted server-side `WidgetRef` snapshot; the model cannot override them.

## Config

`[agent.tool_source_config.widget_data]` is currently empty. The tools depend on `RunContext.widgets` (populated by Workspace's request envelope) and the per-run emit channels.

## Related

- [`inspect_widget_data` tool source](inspect_widget_data.md) — the read-side surface for widgets already materialised this conversation; pairs with `get_widget_data`.
- [`runtime/emit.py`](../../runtime/emit.md) — `FunctionCallSSE` wire shape.
- [`runtime/widget_store.py`](../../runtime/widget_store.md) — where the materialised rows live afterwards.
