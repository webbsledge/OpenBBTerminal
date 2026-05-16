# `openbb_agent_server.runtime.pdf_store`

SQL-backed PDF ingestion + per-page text/word/vector store. Backs the `pdf_extract`, `search_pdf`, and `get_pdf_outline` tools so a long prospectus only gets parsed once per `(user_id, file_key)` and subsequent turns are a SQL read.

**Source:** [`openbb_agent_server/runtime/pdf_store.py`](../../../openbb_agent_server/runtime/pdf_store.py)

## Storage

Two SQLAlchemy tables (see [persistence](../../operating/persistence.md)):

| Table | Holds |
| --- | --- |
| `pdf_documents` | One row per ingested PDF: `name`, `url`, `mime`, `total_pages`, `metadata_json`, `toc_json`, `status` ∈ `{pending, ready, error}`, `error` |
| `pdf_pages` | One row per parsed page: `text`, `words_json` (per-word `{text, x0, top, x1, bottom}`). FK to `pdf_documents.id` with cascade delete. |

When `embeddings` is supplied, an additional `pdf_pages_vec` table (managed by `SQLiteVec`) indexes each page's text for ANN search. The vector table only exists under SQLite; Postgres mode falls back to substring search.

## Class

### `PdfStore(url, *, embeddings=None, engine=None)`

| Constructor arg | Type | Purpose |
| --- | --- | --- |
| `url` | `str` | SQLAlchemy URL (same as the history DB; the store reuses the engine when possible). |
| `embeddings` | `Embeddings \| None` | Optional embeddings backend for page-level ANN search. When `None`, `search()` falls back to substring matching. |
| `engine` | `AsyncEngine \| None` | Pre-built engine to share with the history store; SQLite pragmas are skipped when supplied. |

The `file_key` used to dedupe documents is computed from `(name, url, sha256(data_base64))` so the same PDF re-attached via a different transport returns the cached parse.

## Methods

| Method | Purpose |
| --- | --- |
| `status(principal, name, url?, data_base64?) -> dict \| None` | Lookup the document by `(user_id, file_key)`. Returns `{id, name, url, status, error, total_pages, metadata, toc}` or `None` when nothing has been ingested yet. |
| `get_pages(principal, name, url?, data_base64?, page_range?) -> list[dict] \| None` | Return parsed page rows (`{page, text, words}`), optionally filtered by `(start, end)` inclusive range. Returns `None` if the PDF isn't ingested yet — caller decides whether to wait or fall back to inline parsing. |
| `ingest_async(principal, name, url?, data_base64?) -> str` | Schedule a background parse task for the PDF if one isn't already in flight. Returns the `file_key`. Idempotent: returns the existing task's key when the same `(user_id, file_key)` is already running or `ready`. |
| `await_pending() -> None` | Block until every scheduled background ingestion completes (used by tests and shutdown). |
| `search(principal, query, k=8) -> list[dict]` | ANN search across the user's ingested PDFs. Returns `[{score, name, page, text}]` sorted by relevance. Falls back to a substring scan when no embeddings backend is configured. |

## Background ingestion

`ingest_async` dispatches `_ingest_in_background`, which runs the synchronous `_parse_pdf_sync` helper inside `asyncio.to_thread`. The parser uses `pdfplumber` to extract per-page text + per-word bounding boxes, plus PDF metadata + outline. On success the page rows are written + (if configured) indexed in `pdf_pages_vec`; on failure the document row's `status` becomes `error` with the exception message in `error`.

## Service binding

The store is bound at startup via `services.set_services(pdf=...)` and retrieved by tools via `services.get_pdf_store()` — see [`runtime/services.md`](services.md).
