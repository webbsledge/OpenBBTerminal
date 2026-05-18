"""OpenBB Workspace agent wire-protocol schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer

MessageRole = Literal["human", "ai", "tool"]


class ChatMessage(BaseModel):
    """One message in the multi-turn history."""

    model_config = ConfigDict(extra="allow")

    role: MessageRole
    content: str | dict[str, Any] | None = None
    tool_call_id: str | None = None
    function: str | None = None
    input_arguments: dict[str, Any] | None = None
    data: list[Any] | None = None
    agent_id: str | None = None


class WidgetParam(BaseModel):
    """One parameter advertised by a widget."""

    model_config = ConfigDict(extra="allow")

    name: str
    type: str | None = None
    current_value: Any = None


class WidgetSpec(BaseModel):
    """One workspace-attached widget context entry."""

    model_config = ConfigDict(extra="allow")

    uuid: str | None = None
    widget_id: str | None = None
    name: str | None = None
    type: str | None = None
    origin: str | None = None
    description: str | None = None
    params: list[WidgetParam] | dict[str, Any] = Field(default_factory=list)
    data: Any = None

    @property
    def id(self) -> str:
        """Stable lookup key (uuid when present, else widget_id)."""
        return self.uuid or self.widget_id or ""


class WidgetsBag(BaseModel):
    """Container for primary/secondary/extra widget context."""

    model_config = ConfigDict(extra="allow")

    primary: list[WidgetSpec] = Field(default_factory=list)
    secondary: list[WidgetSpec] = Field(default_factory=list)
    extra: list[WidgetSpec] = Field(default_factory=list)


class UploadedFile(BaseModel):
    """One user-uploaded file (PDF / image / spreadsheet / raw)."""

    model_config = ConfigDict(extra="allow")

    name: str
    mime: str | None = None
    data_base64: str | None = None
    url: str | None = None


class QueryRequest(BaseModel):
    """Body of ``POST /v1/query``."""

    model_config = ConfigDict(extra="allow")

    messages: list[ChatMessage]
    widgets: WidgetsBag = Field(default_factory=WidgetsBag)
    uploaded_files: list[UploadedFile] = Field(default_factory=list)

    api_keys: dict[str, Any] = Field(default_factory=dict)
    api_urls: dict[str, Any] = Field(default_factory=dict)

    workspace_options: list[str] = Field(default_factory=list)

    timezone: str | None = None

    context: list[dict[str, Any]] | None = None
    urls: list[str] | None = None
    force_web_search: bool | None = None
    workspace_state: dict[str, Any] | None = None
    tools: list[dict[str, Any]] | None = None


class BaseSSE(BaseModel):
    """Base class for every SSE event variant."""

    event: str
    data: Any


class MessageChunkSSEData(BaseModel):
    """Payload for ``copilotMessageChunk``."""

    delta: str


class MessageChunkSSE(BaseSSE):
    """Streaming text delta from the model."""

    event: Literal["copilotMessageChunk"] = "copilotMessageChunk"
    data: MessageChunkSSEData


StatusEventType = Literal["INFO", "SUCCESS", "WARNING", "ERROR"]


class StatusUpdateSSEData(BaseModel):
    """Payload for ``copilotStatusUpdate``."""

    eventType: StatusEventType
    message: str
    group: Literal["reasoning"] = "reasoning"
    details: list[dict[str, Any] | str] | None = None
    artifacts: list[ClientArtifact] | None = None
    hidden: bool = False


class StatusUpdateSSE(BaseSSE):
    """A reasoning-step status update."""

    event: Literal["copilotStatusUpdate"] = "copilotStatusUpdate"
    data: StatusUpdateSSEData


FunctionName = Literal[
    "get_widget_data",
    "get_extra_widget_data",
    "get_params_options",
    "add_widget_to_dashboard",
    "add_generative_widget",
    "update_widget_in_dashboard",
    "assign_tasks_to_agents",
    "execute_agent_tool",
    "manage_navigation_bar",
    "get_skill_content",
]


class FunctionCallSSEData(BaseModel):
    """Payload for ``copilotFunctionCall``."""

    function: FunctionName
    input_arguments: dict[str, Any] = Field(default_factory=dict)
    extra_state: dict[str, Any] | None = None


class FunctionCallSSE(BaseSSE):
    """A client-side function call the Workspace UI must execute."""

    event: Literal["copilotFunctionCall"] = "copilotFunctionCall"
    data: FunctionCallSSEData


ArtifactType = Literal[
    "text",
    "table",
    "chart",
    "snowflake_query",
    "snowflake_python",
    "html",
]


class ClientArtifact(BaseModel):
    """The single artifact shape Workspace consumes."""

    type: ArtifactType
    name: str
    description: str
    uuid: str
    content: str | list[dict[str, Any]]
    chart_params: dict[str, Any] | None = None
    query_data_source: dict[str, Any] | None = None


class MessageArtifactSSE(BaseSSE):
    """An artifact the chat panel renders inline."""

    event: Literal["copilotMessageArtifact"] = "copilotMessageArtifact"
    data: ClientArtifact


class CitationHighlightBoundingBox(BaseModel):
    """Pixel bounding box on a PDF page."""

    text: str
    page: int
    x0: float
    top: float
    x1: float
    bottom: float


class SourceInfo(BaseModel):
    """Where a citation came from."""

    model_config = ConfigDict(extra="allow")

    type: Literal["widget", "direct retrieval", "web", "artifact"]
    uuid: str | None = None
    origin: str | None = None
    widget_id: str | None = None
    name: str | None = None
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    citable: bool = True


class Citation(BaseModel):
    """One source attribution."""

    id: str
    source_info: SourceInfo
    details: list[dict[str, Any]] | None = None
    quote_bounding_boxes: list[list[CitationHighlightBoundingBox]] | None = None

    @model_serializer(mode="wrap")
    def _drop_empty_bboxes(self, handler):  # type: ignore[no-untyped-def]
        """Omit ``quote_bounding_boxes`` from the wire payload when null."""
        data = handler(self)
        if data.get("quote_bounding_boxes") is None:
            data.pop("quote_bounding_boxes", None)
        return data


class CitationCollection(BaseModel):
    """Payload for ``copilotCitationCollection``."""

    citations: list[Citation]


class CitationCollectionSSE(BaseSSE):
    """A batch of citations emitted at end-of-run."""

    event: Literal["copilotCitationCollection"] = "copilotCitationCollection"
    data: CitationCollection


SSEEvent = (
    MessageChunkSSE
    | StatusUpdateSSE
    | FunctionCallSSE
    | MessageArtifactSSE
    | CitationCollectionSSE
)


StatusUpdateSSEData.model_rebuild()
