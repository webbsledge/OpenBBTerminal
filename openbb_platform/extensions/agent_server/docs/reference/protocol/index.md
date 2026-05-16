# `openbb_agent_server.protocol`

Wire-protocol layer: typed SSE event shapes, the DeepAgents-to-SSE adapter, and the SSE encoder. Every other module emits dicts via [`runtime/emit.md`](../runtime/emit.md); the adapter is the only piece that knows the Workspace wire shapes.

**Source:** [`openbb_agent_server/protocol/__init__.py`](../../../openbb_agent_server/protocol/__init__.py)

## Pages

| Page | What it covers |
| --- | --- |
| [`schemas.md`](schemas.md) | Every typed event variant — `MessageChunkSSE`, `StatusUpdateSSE`, `FunctionCallSSE`, `MessageArtifactSSE`, `CitationCollectionSSE` — plus `QueryRequest`, `Citation`, `ClientArtifact`. |
| [`adapter.md`](adapter.md) | `DeepAgentEventAdapter` — the streaming translator. Handles `<thinking>` splitting, gpt-oss Harmony-format leak suppression, retroactive prose reclassification when the upstream NVIDIA adapter strips the opening `<think>` tag, and citation buffering. |
| [`sse.md`](sse.md) | `encode_event` / `encode_stream` — UTF-8 SSE frame encoders. |

## Event surface

A typical `/v1/query` exchange streams:

1. (Optional) `copilotStatusUpdate` + `copilotFunctionCall(get_widget_data)` — when the router pre-fetches selected widgets.
2. Zero or more `copilotStatusUpdate` rows — one per reasoning segment between tool calls.
3. Zero or more `copilotFunctionCall` events — only for client-side / MCP / Workspace-native tools; server-side tools run inline.
4. One `copilotMessageChunk` per prose delta in the FINAL answer (chat bubble).
5. Zero or more `copilotMessageArtifact` events — emitted as a tail batch AFTER the final chunk, in arrival order.
6. (Optional) One `copilotCitationCollection` event — every accumulated citation in one frame.

The adapter is responsible for the tail ordering — buffering artifacts so they don't tear the chat bubble's prose flow, and de-duping citations so a re-cited widget produces one chip per page rather than one per call.

## Cancellation

When the FastAPI handler raises `CancelledError` (client disconnect, `/v1/conversations/{id}/cancel`), the adapter's `adapt(...)` async generator unwinds cleanly:

1. Pending splitter / prose / reasoning buffers are dropped.
2. Buffered artifacts and citations are NOT emitted — the SSE stream may have already been closed.
3. The trace is marked `cancelled` by the router's `finally` block via `HistoryStore.end_trace`.

## See also

- [`runtime/emit.md`](../runtime/emit.md) — plugin-side API that produces the events the adapter consumes.
- [`app/router.md`](../app/router.md) — wires the adapter and encoder into the FastAPI response.
- [`guides/architecture.md`](../../guides/architecture.md) — end-to-end event flow.
