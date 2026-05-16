# `openbb_agent_server.protocol.sse`

SSE encoder. Turns the typed `SSEEvent` variants from [`protocol/schemas.md`](schemas.md) into UTF-8 wire frames that conform to the W3C Server-Sent Events spec.

**Source:** [`openbb_agent_server/protocol/sse.py`](../../../openbb_agent_server/protocol/sse.py)

## Frame shape

```
event: <event-name>\n
data: <json-payload>\n
\n
```

`event:` is the `event` field of the `SSEEvent` variant (e.g. `copilotMessageChunk`, `copilotStatusUpdate`, `copilotFunctionCall`). `data:` is `event.data.model_dump_json()` — a single line, no embedded newlines. The trailing blank line terminates the event.

The encoder does NOT emit `id:` or `retry:` lines today — clients reconnect by re-issuing the original `POST /v1/query` rather than resuming via SSE last-event-id.

## Functions

### `def encode_event(event) -> bytes`

Encode one `SSEEvent` as a UTF-8 frame. Returns `bytes` — the FastAPI router yields these directly to the response body.

```python
from openbb_agent_server.protocol.sse import encode_event
from openbb_agent_server.protocol.schemas import (
    MessageChunkSSE, MessageChunkSSEData,
)

frame = encode_event(MessageChunkSSE(data=MessageChunkSSEData(delta="hello ")))
# b"event: copilotMessageChunk\ndata: {\"delta\":\"hello \"}\n\n"
```

### `async def encode_stream(events) -> AsyncIterator[bytes]`

Encode a synchronous or asynchronous iterable of events. Detects `__aiter__` and dispatches accordingly. Used by the router as:

```python
return StreamingResponse(
    encode_stream(adapter.adapt(deep_agent_stream)),
    media_type="text/event-stream",
)
```

## See also

- [`protocol/schemas.md`](schemas.md) — the typed variants.
- [`protocol/adapter.md`](adapter.md) — the source of the events being encoded.
- [`app/router.md`](../app/router.md) — wires this into the response.
