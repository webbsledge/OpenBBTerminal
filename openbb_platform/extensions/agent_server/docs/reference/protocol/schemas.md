# `openbb_agent_server.protocol.schemas`

OpenBB Workspace agent wire-protocol schemas. These Pydantic models are the source of truth this package validates against; they mirror the shapes defined in the [`openbb-ai`](https://github.com/openbb-finance/openbb-ai) SDK.

**Source:** [`openbb_agent_server/protocol/schemas.py`](../../../openbb_agent_server/protocol/schemas.py)

Every model carries `ConfigDict(extra="allow")` unless noted, so Workspace can ship additional fields without breaking validation; the unrecognised fields surface on `model_extra`.

## Inbound request

### `class ChatMessage`

One message in the multi-turn history.

| Field | Type | Purpose |
| --- | --- | --- |
| `role` | `Literal["human", "ai", "tool"]` | Required. Drives downstream parsing of the message. |
| `content` | `str \| dict \| None` | Free-form text or structured content (e.g. envelope dicts on `ai` messages). |
| `tool_call_id` | `str \| None` | Pairs a `tool` message with the assistant call that produced it. |
| `function` | `str \| None` | Tool name on `tool` messages. |
| `input_arguments` | `dict \| None` | Arguments echoed back on `tool` messages. |
| `data` | `list \| None` | Tool result payload, when not embedded in `content`. |
| `agent_id` | `str \| None` | Multi-agent attribution. |

### `class WidgetParam`

| Field | Type | Purpose |
| --- | --- | --- |
| `name` | `str` | Param name (`ticker`, `doc_type`). |
| `type` | `str \| None` | Optional Workspace param type hint. |
| `current_value` | `Any` | Current selection. |

### `class WidgetSpec`

One workspace-attached widget context entry.

| Field | Type | Purpose |
| --- | --- | --- |
| `uuid` | `str \| None` | Per-instance dashboard UUID. |
| `widget_id` | `str \| None` | Internal slug; shared across instances. |
| `name` | `str \| None` | Display label. |
| `type` | `str \| None` | Widget type tag. |
| `origin` | `str \| None` | Provider origin. |
| `description` | `str \| None` | Long description. |
| `params` | `list[WidgetParam] \| dict` | Either the structured param list or a flat key→value dict. |
| `data` | `Any` | Pre-fetched rows when available. |

`WidgetSpec.id` returns the stable lookup key (`uuid` when present, else `widget_id`).

### `class WidgetsBag`

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `primary` | `list[WidgetSpec]` | `[]` | Pinned + currently-selected. |
| `secondary` | `list[WidgetSpec]` | `[]` | Other dashboard widgets. |
| `extra` | `list[WidgetSpec]` | `[]` | Workspace-suggested. |

### `class UploadedFile`

| Field | Type | Purpose |
| --- | --- | --- |
| `name` | `str` | Filename. |
| `mime` | `str \| None` | MIME type. |
| `data_base64` | `str \| None` | Inline bytes. |
| `url` | `str \| None` | Fetchable URL. |

### `class QueryRequest`

Body of `POST /v1/query`.

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `messages` | `list[ChatMessage]` | required | Full multi-turn history. |
| `widgets` | `WidgetsBag` | empty bag | Selected / pinned / extra widget context. |
| `uploaded_files` | `list[UploadedFile]` | `[]` | Files attached this turn. |
| `api_keys` | `dict` | `{}` | Per-request keys forwarded into `RunContext`. |
| `api_urls` | `dict` | `{}` | Per-request base URLs. |
| `workspace_options` | `list[str]` | `[]` | User-toggled custom features (`deep-research`, `search-web`, …). |
| `timezone` | `str \| None` | `None` | IANA TZ. |
| `context` | `list[dict] \| None` | `None` | Additional Workspace-supplied context blobs. |
| `urls` | `list[str] \| None` | `None` | URLs the agent should consider. |
| `force_web_search` | `bool \| None` | `None` | User toggled the Search-Web feature. |
| `workspace_state` | `dict \| None` | `None` | Dashboard state snapshot. |
| `tools` | `list[dict] \| None` | `None` | The user's enabled MCP tool list. |

Additional fields (e.g. `run_id`, `conversation_id`) ride on `extra="allow"` — the router reads them off `model_extra`.

## SSE wire types

### `class BaseSSE`

```python
class BaseSSE(BaseModel):
    event: str
    data: Any
```

Each subclass below fixes `event` to one of Workspace's recognised values.

### `MessageChunkSSE` — `event="copilotMessageChunk"`

| `data` field | Type | Purpose |
| --- | --- | --- |
| `delta` | `str` | One text delta from the streaming model. |

### `StatusUpdateSSE` — `event="copilotStatusUpdate"`

| `data` field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `eventType` | `Literal["INFO","SUCCESS","WARNING","ERROR"]` | required | Severity. |
| `message` | `str` | required | One reasoning row. |
| `group` | `Literal["reasoning"]` | `"reasoning"` | Workspace renders this in the reasoning panel. |
| `details` | `list[dict \| str] \| None` | `None` | Extra rows shown when the user expands the entry. |
| `artifacts` | `list[ClientArtifact] \| None` | `None` | Artifacts to render alongside this reasoning row. |
| `hidden` | `bool` | `False` | Hide from the reasoning panel. |

### `FunctionCallSSE` — `event="copilotFunctionCall"`

| `data` field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `function` | `Literal[...]` | required | One of `get_widget_data`, `get_extra_widget_data`, `get_params_options`, `add_widget_to_dashboard`, `add_generative_widget`, `update_widget_in_dashboard`, `assign_tasks_to_agents`, `execute_agent_tool`, `manage_navigation_bar`, `get_skill_content`. |
| `input_arguments` | `dict` | `{}` | Arguments the Workspace UI passes to the function. |
| `extra_state` | `dict \| None` | `None` | Echoed back to the agent on the next request. |

### `MessageArtifactSSE` — `event="copilotMessageArtifact"`

`data` is a `ClientArtifact`:

| Field | Type | Purpose |
| --- | --- | --- |
| `type` | `Literal["text","table","chart","snowflake_query","snowflake_python","html"]` | Workspace's recognised artifact set. `markdown`, `file`, and `image` are NOT supported — surface them as `text` / `html`. |
| `name` | `str` | Artifact title. |
| `description` | `str` | Subtitle / hover text. |
| `uuid` | `str` | Stable per-artifact UUID. |
| `content` | `str \| list[dict]` | The artifact body (string for text/html; rows for table). |
| `chart_params` | `dict \| None` | Plotly figure args for chart artifacts. |
| `query_data_source` | `dict \| None` | Snowflake-specific routing for query artifacts. |

### `CitationCollectionSSE` — `event="copilotCitationCollection"`

`data` is a `CitationCollection` (`{ citations: list[Citation] }`). The adapter buffers all citations across the run and emits ONE collection event at end-of-run.

#### `class CitationHighlightBoundingBox`

| Field | Type | Purpose |
| --- | --- | --- |
| `text` | `str` | Label shown on the highlight chip. |
| `page` | `int` | 1-based page number. |
| `x0` / `top` / `x1` / `bottom` | `float` | Pixel coordinates of the highlight rectangle. |

#### `class SourceInfo`

`extra="allow"`. Where a citation came from.

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `type` | `Literal["widget","direct retrieval","web","artifact"]` | required | Drives chip rendering. `widget` chips resolve to a pinned dashboard widget; `web` chips render a URL. |
| `uuid` | `str \| None` | `None` | Per-citation identifier (Workspace uses `metadata.widget_uuid` to match the dashboard widget — set both). |
| `origin` | `str \| None` | `None` | Source URL / origin label. |
| `widget_id` | `str \| None` | `None` | Internal widget slug. |
| `name` | `str \| None` | `None` | Chip title. |
| `description` | `str \| None` | `None` | Hover text. |
| `metadata` | `dict` | `{}` | Workspace reads `metadata.widget_uuid` to bind the chip to a pinned widget; `metadata.input_args` is rendered in the popup. |
| `citable` | `bool` | `True` | Whether Workspace renders a clickable chip. |

#### `class Citation`

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | `str` | Per-citation UUID. |
| `source_info` | `SourceInfo` | Where the citation came from. |
| `details` | `list[dict] \| None` | Rows shown in the chip popup (`{Name, Filename, Page}` for PDFs; `{text, url, title}` for web). |
| `quote_bounding_boxes` | `list[list[CitationHighlightBoundingBox]] \| None` | Outer list = quote groups; inner = per-line bboxes for one quote. A flat single-level list is rejected by Workspace's PDF viewer. The field is omitted (not `null`) on the wire when unset. |

## Union

```python
SSEEvent = (
    MessageChunkSSE
    | StatusUpdateSSE
    | FunctionCallSSE
    | MessageArtifactSSE
    | CitationCollectionSSE
)
```

The adapter type-narrows on this union before encoding each event to wire format via [`protocol/sse.py`](sse.md).
