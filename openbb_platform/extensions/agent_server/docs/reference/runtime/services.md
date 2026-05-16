# `openbb_agent_server.runtime.services`

Shared service slots populated at app startup.

**Source:** [`openbb_agent_server/runtime/services.py`](../../../openbb_agent_server/runtime/services.py)

## Functions

### `def set_services(*, history, memory, checkpointer, **extra)`

Bind shared services at startup. Call exactly once.

### `def get_history()`

Return the bound :class:`HistoryStore` or raise.

### `def get_memory()`

Return the bound :class:`MemoryStore`, or ``None`` if memory is disabled.

### `def get_widget_store()`

Return the bound :class:`WidgetDataStore`, or ``None`` if absent.

### `def get_pdf_store()`

Return the bound :class:`PdfStore`, or ``None`` if PDF ingestion is disabled. The store backs the `pdf_documents` / `pdf_pages` tables and is consulted by `pdf_extract`, `search_pdf`, and `get_pdf_outline`.

### `def get_checkpointer()`

Return the bound LangGraph checkpointer.

### `def reset()`

Test-only: forget all bound services.
