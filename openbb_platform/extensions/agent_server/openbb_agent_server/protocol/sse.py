"""SSE encoder."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable

from openbb_agent_server.protocol.schemas import SSEEvent


def encode_event(event: SSEEvent) -> bytes:
    """Encode one event as a UTF-8 SSE frame."""
    payload = event.data.model_dump_json()
    frame = f"event: {event.event}\ndata: {payload}\n\n"
    return frame.encode("utf-8")


async def encode_stream(
    events: Iterable[SSEEvent] | AsyncIterator[SSEEvent],
) -> AsyncIterator[bytes]:
    """Encode a (sync or async) iterable of events into SSE frames."""
    from typing import cast

    if hasattr(events, "__aiter__"):
        async for ev in cast(AsyncIterator[SSEEvent], events):
            yield encode_event(ev)
    else:
        for ev in cast(Iterable[SSEEvent], events):
            yield encode_event(ev)
