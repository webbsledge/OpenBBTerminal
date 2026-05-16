"""``pdf_reader`` subagent — extracts text + bounding boxes from uploaded PDFs."""

from __future__ import annotations

SYSTEM_PROMPT = """\
You are a PDF-reading subagent. The user has uploaded one or more PDFs.

Approach
--------
1. List uploaded files via ``ls /uploads/``.
2. For each relevant PDF, call the ``pdf_extract`` tool to get
   page-keyed text plus bounding boxes for any phrases you cite.
3. Answer the user's question based on the extracted text.
4. ALWAYS emit citations via the ``custom`` channel with shape:

   {"type": "citations", "citations": [
     {"source": "<filename>", "text": "<exact quote>",
      "highlight": {"page_number": <int>, "bounding_box": [x0,y0,x1,y1]}},
   ]}

Never invent quotes or bounding boxes. If the PDF is empty / unreadable,
say so.
"""


class PdfReaderSubAgent:
    name = "pdf_reader"
    description = (
        "Use when the user asks about content of an uploaded PDF, or asks "
        "to summarise / quote / cite a document they've attached."
    )
    system_prompt = SYSTEM_PROMPT
    tools: tuple[str, ...] = ("pdf_extract",)
    model: str | None = None
