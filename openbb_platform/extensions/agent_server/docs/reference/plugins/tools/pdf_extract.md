# `openbb_agent_server.plugins.tools.pdf_extract`

PDF tool source — text extraction, table-of-contents lookup, full-text search, and per-page word bounding boxes for citation highlights. Backed by `pdfplumber`; results are cached in the `pdf_documents` / `pdf_pages` tables (see [persistence](../../../operating/persistence.md)) so the second turn on the same PDF is a single SQL read.

**Source:** [`openbb_agent_server/plugins/tools/pdf_extract.py`](../../../../openbb_agent_server/plugins/tools/pdf_extract.py)

## Classes

### `PdfExtractToolSource`

Plugin entry-point name: `pdf_extract`. `tools(ctx, config)` registers four `StructuredTool`s:

| Tool | Args | Returns |
| --- | --- | --- |
| `list_pdfs()` | — | `{count, pdfs: [{name, mime, url}], document_widgets?}` — the user's attached PDFs this turn. One-shot per turn; a second call returns a STOP message. When the list is empty but document-bearing widgets are on the dashboard, returns `document_widgets` so the agent can call `get_widget_data` to materialise their bytes. |
| `get_pdf_outline(name)` | `name: str` (fuzzy-matched filename) | `{name, total_pages, metadata, toc}` — cheapest navigation primitive. No page text. |
| `search_pdf(query, k=8)` | `query: str`, `k: int ∈ [1, 50]` | List of `{score, name, page, text}` page hits sorted by relevance. Vectors are built in the background at request ingest time. Auto-emits one widget-anchored citation per unique `(pdf, page)` hit. |
| `pdf_extract(name, page_range=None, include_words=False)` | `name: str`, `page_range: (int, int) \| None` (1-based inclusive), `include_words: bool` | `{name, total_pages, page_range, is_preview, metadata, toc, pages: [{page, text, words?}]}`. Without `page_range`, returns a preview of the first three pages. `include_words=True` roughly 10×s response size. Auto-emits one widget-anchored citation. |

`page_range` accepts `[a, b]`, `(a, b)`, `"a-b"`, `"a,b"`, `"a..b"`, or a single integer — the args validator normalises all of these into the canonical tuple before the tool runs.

### Citation emission

`pdf_extract` and `search_pdf` automatically call `emit.cite()` once per unique `(pdf, page)` they read. The citation chip resolves to a pinned widget instance by matching PDF filename tokens against widget params (ticker / fund-id / doc-type) — `_content_match` is the disambiguator when multiple instances share a `widget_id` slug. See [`runtime/emit.py`](../../runtime/emit.md) for the wire shape.

## Config

`[agent.tool_source_config.pdf_extract]` is currently empty — the tool inspects `RunContext.uploaded_files` and resolves PDF bytes from `data_base64` / `url` / extra fields without configuration.
