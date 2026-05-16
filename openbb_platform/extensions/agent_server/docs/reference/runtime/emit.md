# `openbb_agent_server.runtime.emit`

Tool-side helpers for emitting OpenBB Workspace SSE events. Each helper looks up the active stream writer (LangGraph's `get_stream_writer()`, or a `bind_writer()` override in tests) and writes a `dict` payload that the adapter at `protocol/adapter.py` converts into the wire-typed `SSEEvent` variants.

**Source:** [`openbb_agent_server/runtime/emit.py`](../../../openbb_agent_server/runtime/emit.py) · **Wire shapes:** [`protocol/schemas.md`](../protocol/schemas.md)

## Writer binding

### `bind_writer(sink) -> Iterator`

Context manager. Binds `sink: Callable[[dict], None]` as the writer for the lifetime of the `with` block. Used by tests to capture events; in production the writer comes from LangGraph.

## Reasoning steps

### `reasoning_step(message, *, event_type="INFO", **details) -> None`

Emit a single reasoning row (`copilotStatusUpdate`). `event_type` ∈ `INFO` / `SUCCESS` / `WARNING` / `ERROR`. `**details` becomes the `details` field on the SSE event.

## Artifacts

Each artifact helper returns the artifact UUID (generated if not supplied) so the caller can reference it later. All emit `copilotMessageArtifact` SSE events.

### `html_artifact(*, content, name="", description="", uuid=None) -> str`

Emit an HTML artifact. `content` is sanitised by Workspace (no scripts, no iframes).

### `markdown_artifact(*, content, name="", description="", uuid=None) -> str`

Emit a markdown artifact. Surfaced as `type="text"` on the wire (Workspace's `ArtifactType` enum has no `markdown` member).

### `table_artifact(*, columns, rows, name="", description="", uuid=None) -> str`

Emit a tabular artifact. `columns: list[str]`, `rows: list[list[Any]]`.

### `chart_artifact(*, plotly, name="", description="", uuid=None) -> str`

Emit a chart artifact. `plotly` is a Plotly figure JSON.

### `image_artifact(*, name="", description="", mime="image/png", data_base64=None, url=None, uuid=None) -> str`

Emit an image. Wrapped as an HTML artifact with an `<img>` tag because Workspace has no native image type. Either `data_base64` or `url` is required.

### `file_artifact(*, name="", description="", mime="application/octet-stream", data_base64=None, url=None, uuid=None) -> str`

Emit a file as an HTML artifact wrapping a `<a download>` link. Either `data_base64` or `url` is required.

## Citations

### `cite(*, text=None, source=None, source_url=None, quote_bounding_boxes=None, widget=None, widget_id=None, input_arguments=None, extra_details=None) -> None`

Buffer one citation. The adapter flushes the full citation list as a single `copilotCitationCollection` SSE at end of run.

| Arg | Purpose |
| --- | --- |
| `text` | Quote text for non-widget (web) citations. Goes into `details[0].text`. |
| `source` | Human label for the citation source (chip title). |
| `source_url` | URL for web citations; `origin` for widget citations. |
| `widget` | Per-instance dashboard widget UUID. When set, the citation becomes `type="widget"` and the chip resolves to the pinned widget. Required for PDF / document citations. |
| `widget_id` | Internal widget slug (e.g. `blk_drill_fund_documents`). Secondary label on the chip; helpful for analytics but not used by the chip resolver. |
| `input_arguments` | What the widget was called with — rendered as `metadata.input_args` on the wire. Workspace displays each k/v in the chip popup. |
| `extra_details` | Extra row appended to `details` (e.g. `{Name, Filename, Page}`). |
| `quote_bounding_boxes` | `list[list[CitationHighlightBoundingBox]]` — outer list is quote groups, inner is per-line bboxes for that quote. Drives the highlight-and-navigate behaviour in Workspace's PDF viewer. A flat single-level list does NOT work. |

The full wire shape lives at [`protocol/schemas.md#citation`](../protocol/schemas.md#citation).

## Client-side function calls

### `function_call(*, tool_name, parameters=None, server_id="agent", call_id=None) -> str`

Emit `copilotFunctionCall`. Returns the call_id (generated if not supplied) so the round-trip on the next request can be matched back to the dispatch.
