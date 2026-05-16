"""Shared service slots populated at app startup."""

from __future__ import annotations

from typing import Any

from openbb_agent_server.memory.store import MemoryStore
from openbb_agent_server.persistence.store import HistoryStore


class _Services:
    history: HistoryStore | None = None
    memory: MemoryStore | None = None
    checkpointer: Any = None
    extra: dict[str, Any] = {}


_services = _Services()


def set_services(
    *,
    history: HistoryStore | None = None,
    memory: MemoryStore | None = None,
    checkpointer: Any = None,
    **extra: Any,
) -> None:
    """Bind shared services at startup. Call exactly once."""
    if history is not None:
        _services.history = history
    if memory is not None:
        _services.memory = memory
    if checkpointer is not None:
        _services.checkpointer = checkpointer
    _services.extra.update(extra)


def get_history() -> HistoryStore:
    """Return the bound :class:`HistoryStore` or raise."""
    if _services.history is None:
        raise RuntimeError("HistoryStore not bound; call set_services() at startup")
    return _services.history


def get_memory() -> MemoryStore | None:
    """Return the bound :class:`MemoryStore`, or ``None`` if memory is disabled."""
    return _services.memory


def get_widget_store() -> Any:
    """Return the bound :class:`WidgetDataStore`, or ``None`` if absent."""
    return _services.extra.get("widget_store")


def get_pdf_store() -> Any:
    """Return the bound :class:`PdfStore`, or ``None`` if absent."""
    return _services.extra.get("pdf_store")


def get_checkpointer() -> Any:
    """Return the bound LangGraph checkpointer."""
    if _services.checkpointer is None:
        raise RuntimeError(
            "Checkpointer not bound; the app must configure one at startup"
        )
    return _services.checkpointer


def reset() -> None:
    """Test-only: forget all bound services."""
    _services.history = None
    _services.memory = None
    _services.checkpointer = None
    _services.extra.clear()
