"""``inmemory`` — :class:`langgraph.checkpoint.memory.InMemorySaver`."""

from __future__ import annotations

from typing import Any

from langgraph.checkpoint.memory import InMemorySaver

from openbb_agent_server.runtime.plugins import CheckpointerProvider


class InMemoryCheckpointerProvider(CheckpointerProvider):
    """Construct a process-local :class:`InMemorySaver`."""

    name = "inmemory"

    def __init__(self, **_config: Any) -> None:
        # ``InMemorySaver`` takes no constructor args we care about.
        pass

    async def open(self, settings: Any) -> InMemorySaver:
        return InMemorySaver()

    async def close(self, saver: Any) -> None:
        return None
