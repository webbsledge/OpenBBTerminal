# `openbb_agent_server.plugins.subagents.pdf_reader`

Extracts structured text — including per-phrase bounding boxes for citation highlighting — from one or more PDFs the user has attached to the conversation. The parent agent reaches here whenever the question is about the contents of an attached document (summarise, quote, cite, locate a specific clause).

**Source:** [`openbb_agent_server/plugins/subagents/pdf_reader.py`](../../../../openbb_agent_server/plugins/subagents/pdf_reader.py)

## System prompt

Defined verbatim in `SYSTEM_PROMPT`. The pdf_reader follows a four-step procedure:

1. List uploaded files via `ls /uploads/` (i.e. inspect `RunContext.uploaded_files`).
2. For each relevant PDF, call the `pdf_extract` tool to get page-keyed text plus bounding boxes for any phrases that will be cited.
3. Answer the user's question using the extracted text.
4. **Always** emit citations via the `custom` stream channel with shape:

   ```json
   {"type": "citations", "citations": [
     {"source": "<filename>", "text": "<exact quote>",
      "highlight": {"page_number": <int>, "bounding_box": [x0, y0, x1, y1]}}
   ]}
   ```

The prompt is explicit: **never invent quotes or bounding boxes**. If a PDF is empty or unreadable, the subagent must say so. This rule is what makes the pdf_reader different from the [`researcher`](researcher.md) — its citations resolve back to a pixel rectangle inside a known document, not to an external URL.

## Classes

### `PdfReaderSubAgent`

| Attribute | Value |
| --- | --- |
| `name` | `"pdf_reader"` |
| `description` | `"Use when the user asks about content of an uploaded PDF, or asks to summarise / quote / cite a document they've attached."` |
| `system_prompt` | `SYSTEM_PROMPT` (see above) |
| `tools` | `("pdf_extract",)` — narrowed to just the PDF surface |
| `model` | `None` — inherits the parent's model |

This is the only built-in subagent that pins a non-empty `tools` tuple. The narrowing is intentional: it stops the model from reaching for `web_search` or other retrieval tools when the answer is already inside the attached document. The parent still resolves the actual tool implementations, so `pdf_extract` here points to whatever the parent has registered under that name (in practice, [`pdf_extract`](../tools/pdf_extract.md) and its companions `list_pdfs`, `get_pdf_outline`, `search_pdf`).

## How to register

```toml
[project.entry-points."openbb_agent_server.subagents"]
pdf_reader = "openbb_agent_server.plugins.subagents.pdf_reader:PdfReaderSubAgent"
```

## When the parent invokes this

- "What does the prospectus say about fees?"
- "Summarise this 10-K."
- "Find the section about insider transactions in the document I uploaded."
- "Quote the exact language on revenue recognition from the policy."

The parent detects that the user has at least one attached PDF (`RunContext.uploaded_files`) and that the question is about its contents, then hands off the question verbatim.

## Related

- [`pdf_extract` tool source](../tools/pdf_extract.md) — the tool surface this subagent inherits, including `pdf_extract`, `list_pdfs`, `get_pdf_outline`, and `search_pdf` (which auto-cites).
- [`researcher` subagent](researcher.md) — sibling for external retrieval; use when no PDF is attached.
- [Writing a subagent](../../../developing/writing-a-subagent.md).
