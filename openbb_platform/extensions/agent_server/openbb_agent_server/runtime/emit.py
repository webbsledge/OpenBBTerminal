"""Tool-side helpers for emitting OpenBB Workspace SSE events."""

from __future__ import annotations

import contextvars
import logging
import secrets
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger("openbb_agent_server.emit")


_writer_override: contextvars.ContextVar[Callable[[dict[str, Any]], None] | None] = (
    contextvars.ContextVar("openbb_agent_server.emit_writer", default=None)
)


@contextmanager
def bind_writer(
    sink: Callable[[dict[str, Any]], None],
) -> Iterator[Callable[[dict[str, Any]], None]]:
    """Bind ``sink`` as the emit writer for the lifetime of the ``with`` block."""
    token = _writer_override.set(sink)
    try:
        yield sink
    finally:
        _writer_override.reset(token)


def _writer() -> Any:
    """Return the writer for the current call, or ``None``."""
    override = _writer_override.get()
    if override is not None:
        return override
    try:
        from langgraph.config import get_stream_writer
    except ImportError:  # pragma: no cover — langgraph is a hard dep
        return None
    try:
        return get_stream_writer()
    except (LookupError, RuntimeError):
        return None


def _new_uuid() -> str:
    return secrets.token_urlsafe(12)


def reasoning_step(
    message: str,
    *,
    event_type: str = "INFO",
    **details: Any,
) -> None:
    """Emit a reasoning step. ``event_type`` ∈ INFO/SUCCESS/WARNING/ERROR."""
    w = _writer()
    if w is None:
        logger.warning("reasoning_step: no stream writer; would emit %s", message)
        return
    w(
        {
            "type": "step",
            "event_type": event_type,
            "message": message,
            "details": dict(details),
        }
    )


def _emit_artifact(payload: dict[str, Any]) -> None:
    w = _writer()
    if w is None:
        logger.warning("artifact: no stream writer; would emit %s", payload.get("name"))
        return
    w({"type": "artifact", "artifact": payload})


def html_artifact(
    *,
    content: str,
    name: str = "",
    description: str = "",
    uuid: str | None = None,
) -> str:
    """Emit an HTML artifact. Returns the artifact uuid for later reference."""
    artifact_uuid = uuid or _new_uuid()
    _emit_artifact(
        {
            "type": "html",
            "uuid": artifact_uuid,
            "name": name,
            "description": description,
            "content": content,
        }
    )
    return artifact_uuid


def markdown_artifact(
    *,
    content: str,
    name: str = "",
    description: str = "",
    uuid: str | None = None,
) -> str:
    """Emit a markdown artifact."""
    artifact_uuid = uuid or _new_uuid()
    _emit_artifact(
        {
            "type": "markdown",
            "uuid": artifact_uuid,
            "name": name,
            "description": description,
            "content": content,
        }
    )
    return artifact_uuid


def table_artifact(
    *,
    columns: list[str],
    rows: list[list[Any]],
    name: str = "",
    description: str = "",
    uuid: str | None = None,
) -> str:
    """Emit a table artifact."""
    artifact_uuid = uuid or _new_uuid()
    _emit_artifact(
        {
            "type": "table",
            "uuid": artifact_uuid,
            "name": name,
            "description": description,
            "columns": columns,
            "rows": rows,
        }
    )
    return artifact_uuid


def chart_artifact(
    *,
    plotly: dict[str, Any],
    name: str = "",
    description: str = "",
    uuid: str | None = None,
) -> str:
    """Emit a chart artifact (Plotly figure JSON)."""
    artifact_uuid = uuid or _new_uuid()
    _emit_artifact(
        {
            "type": "chart",
            "uuid": artifact_uuid,
            "name": name,
            "description": description,
            "plotly": plotly,
        }
    )
    return artifact_uuid


def image_artifact(
    *,
    name: str = "",
    description: str = "",
    mime: str = "image/png",
    data_base64: str | None = None,
    url: str | None = None,
    uuid: str | None = None,
) -> str:
    """Emit an image as an HTML artifact (the only image-capable wire type)."""
    if not (data_base64 or url):
        raise ValueError("image_artifact requires data_base64 or url")
    src = url or f"data:{mime};base64,{data_base64}"
    artifact_uuid = uuid or _new_uuid()
    alt = name or description or "image"
    _emit_artifact(
        {
            "type": "html",
            "uuid": artifact_uuid,
            "name": name,
            "description": description,
            "content": f'<img src="{src}" alt="{alt}" />',
        }
    )
    return artifact_uuid


def file_artifact(
    *,
    name: str = "",
    description: str = "",
    mime: str = "application/octet-stream",
    data_base64: str | None = None,
    url: str | None = None,
    uuid: str | None = None,
) -> str:
    """Emit a file as an HTML artifact wrapping a download link."""
    if not (data_base64 or url):
        raise ValueError("file_artifact requires data_base64 or url")
    href = url or f"data:{mime};base64,{data_base64}"
    artifact_uuid = uuid or _new_uuid()
    label = name or "Download"
    _emit_artifact(
        {
            "type": "html",
            "uuid": artifact_uuid,
            "name": name,
            "description": description,
            "content": f'<a href="{href}" download="{name}">{label}</a>',
        }
    )
    return artifact_uuid


def cite(
    *,
    text: str | None = None,
    source: str | None = None,
    source_url: str | None = None,
    quote_bounding_boxes: list[list[dict[str, Any]]] | None = None,
    widget: str | None = None,
    widget_id: str | None = None,
    input_arguments: dict[str, Any] | None = None,
    extra_details: dict[str, Any] | None = None,
) -> None:
    """Emit one citation. The router buffers and emits as a single batch."""
    w = _writer()
    if w is None:
        logger.warning("cite: no stream writer; would emit %s", source or source_url)
        return

    source_info: dict[str, Any]
    if widget:
        source_info = {
            "type": "widget",
            "uuid": widget,
            "widget_id": widget_id,
            "name": source,
            "origin": source_url,
        }
    else:
        source_info = {
            "type": "web",
            "name": source,
            "origin": source_url,
        }
    if widget or input_arguments:
        metadata: dict[str, Any] = {}
        if widget:
            metadata["widget_uuid"] = widget
        if input_arguments:
            metadata["input_args"] = input_arguments
        source_info["metadata"] = metadata

    details: list[dict[str, Any]] | None
    details_entries: list[dict[str, Any]] = []
    if extra_details:
        details_entries.append(dict(extra_details))
    if not widget:
        details_entry: dict[str, Any] = {}
        if text:
            details_entry["text"] = text
        if source_url:
            details_entry["url"] = source_url
        if source:
            details_entry["title"] = source
        if details_entry:
            details_entries.append(details_entry)
    details = details_entries or None

    citation: dict[str, Any] = {
        "id": _new_uuid(),
        "source_info": source_info,
        "details": details,
    }
    if quote_bounding_boxes is not None:
        citation["quote_bounding_boxes"] = quote_bounding_boxes
    w({"type": "citations", "citations": [citation]})


def function_call(
    *,
    tool_name: str,
    parameters: dict[str, Any] | None = None,
    server_id: str = "agent",
    call_id: str | None = None,
) -> str:
    """Ask the Workspace UI to execute a client-side tool."""
    cid = call_id or _new_uuid()
    w = _writer()
    if w is None:
        logger.warning("function_call: no stream writer; would emit %s", tool_name)
        return cid
    w(
        {
            "type": "function_call",
            "server_id": server_id,
            "tool_name": tool_name,
            "parameters": parameters or {},
            "call_id": cid,
        }
    )
    return cid
