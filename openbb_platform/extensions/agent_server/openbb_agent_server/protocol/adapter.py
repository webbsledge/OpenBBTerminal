"""DeepAgents stream → OpenBB SSE adapter."""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator
from typing import Any, Literal, cast

logger = logging.getLogger("openbb_agent_server.protocol.adapter")

from openbb_agent_server.observability.logging import trace
from openbb_agent_server.protocol.schemas import (
    Citation,
    CitationCollection,
    CitationCollectionSSE,
    ClientArtifact,
    FunctionCallSSE,
    FunctionCallSSEData,
    MessageArtifactSSE,
    MessageChunkSSE,
    MessageChunkSSEData,
    SSEEvent,
    StatusUpdateSSE,
    StatusUpdateSSEData,
)

_THINKING_RE = re.compile(
    r"<(?P<tag>thinking|think|reasoning)\b[^>]*>(?P<body>.*?)</(?P=tag)>",
    re.DOTALL | re.IGNORECASE,
)

_THINKING_OPEN_RE = re.compile(
    r"<(thinking|think|reasoning)\b[^>]*>",
    re.IGNORECASE,
)

_THINKING_CLOSE_RE = re.compile(
    r"</(thinking|think|reasoning)\s*>",
    re.IGNORECASE,
)

# Longest possible *partial* thinking-tag prefix we hold back while
# waiting for the rest of the tag to arrive. ``</reasoning>`` is 12
# chars; round up to cover arbitrary tag attributes.
_PARTIAL_TAG_HOLD = 32

# Inline citation markers a model emits as TEXT instead of calling the
# ``cite_source`` tool. Two leaked shapes, both stripped from prose
# before it reaches the chat bubble:
#   1. keyword markers — ``【cite_source text="…" source="…"】``
#   2. bare citation-id refs — ``【rCW9mowjZMqwr7hu】`` — the opaque
#      ``secrets.token_urlsafe`` ids ``emit.cite`` hands back, which a
#      model sometimes echoes inline instead of calling ``cite_source``.
#      8+ chars of pure url-safe-base64 (no spaces, no CJK) inside the
#      brackets is the id signal; real ``【…】`` prose has neither.
_CITATION_MARKER_RE = re.compile(
    r"【(?:[^】]*(?:cursor|loc|source|ref|cite)[^】]*|[A-Za-z0-9_-]{8,})】",
    re.IGNORECASE,
)

# Matches the first sign that gpt-oss is emitting raw OpenAI Harmony
# format inline — either the ``【assistant to=functions.X`` tool-call
# header or any of the ``<|channel|>`` / ``<|start|>`` channel
# delimiters. The langchain_nvidia_ai_endpoints adapter does not parse
# this format into LangChain tool_calls, so the markers leak as plain
# text into the chat bubble. They always appear AFTER the final answer
# (gpt-oss emits the visible answer first, then the tool-call message),
# so once any marker is seen we suppress the remainder of the message.
_HARMONY_TRIGGER_RE = re.compile(
    r"(?:【\s*assistant\s+to=functions\.|<\|(?:channel|start)\|>)",
    re.IGNORECASE,
)


class _ThinkingStreamSplitter:
    """Stateful streaming splitter for inline ``<thinking>`` markers.

    The model emits prose with embedded ``<thinking>...</thinking>``
    (or ``<think>``, ``<reasoning>``) blocks. The splitter consumes text
    deltas and yields ``(channel, text)`` pairs where ``channel`` is
    ``"prose"`` or ``"thinking"``. It holds back the tail of each delta
    only when that tail could be a partial tag, so first-token latency
    is one chunk, not one full AIMessage.
    """

    def __init__(self) -> None:
        self._buf: str = ""
        self._in_thinking: bool = False
        # Track whether we've ever seen an opening ``<think>``-class
        # tag. The LangChain NVIDIA adapter
        # (``langchain_nvidia_ai_endpoints``) parses ``<think>``
        # blocks out of the streamed content: it populates
        # ``additional_kwargs.reasoning_content`` AND strips the
        # opening tag from content, but preserves the closing
        # ``</think>``. Without this flag we'd never enter thinking
        # mode and the whole reasoning block would route to the
        # chat bubble.
        self._saw_open_tag: bool = False
        # Single-shot — only the first stray close per splitter
        # triggers a retroactive reclassification. If the model
        # legitimately interleaves thinking + prose later in the
        # turn we don't keep moving prose around.
        self._signalled_implicit: bool = False
        # Once gpt-oss starts emitting OpenAI Harmony tool-call
        # markup we drop every subsequent byte for this message.
        self._harmony_suppress: bool = False

    def feed(self, delta: str) -> list[tuple[str, str]]:
        """Feed a text delta. Returns zero or more ``(channel, text)`` pairs.

        Hold-back is *only* the characters that could still complete a
        partial tag at the buffer's tail (up to ``_PARTIAL_TAG_HOLD``).
        Everything else flushes on the same tick. If the buffer doesn't
        end on a ``<``, nothing is held.

        Stray ``</think>`` tags in prose mode (no matching open seen)
        signal that the opening tag was stripped upstream. The text
        preceding the close is reclassified as ``thinking`` AND a
        ``("close_unmatched", "")`` marker is emitted once so the
        adapter can move already-buffered prose to the reasoning
        buffer.
        """
        if not delta or self._harmony_suppress:
            return []
        self._buf += delta
        # Strip the OpenAI Harmony tool-call leak. The marker always
        # marks the start of an un-parseable tail — drop everything
        # from the marker onwards and refuse further deltas.
        harmony_match = _HARMONY_TRIGGER_RE.search(self._buf)
        if harmony_match:
            self._buf = self._buf[: harmony_match.start()]
            self._harmony_suppress = True
        out: list[tuple[str, str]] = []
        while self._buf:
            channel = "thinking" if self._in_thinking else "prose"
            if self._in_thinking:
                m = _THINKING_CLOSE_RE.search(self._buf)
                toggles_state = True
            else:
                open_m = _THINKING_OPEN_RE.search(self._buf)
                close_m = _THINKING_CLOSE_RE.search(self._buf)
                if open_m and (not close_m or open_m.start() <= close_m.start()):
                    m = open_m
                    toggles_state = True
                    self._saw_open_tag = True
                elif close_m:
                    m = close_m
                    toggles_state = False
                    # Stray close with no preceding open — the
                    # upstream NVIDIA adapter ate the open tag.
                    # Reclassify the preceding content as thinking
                    # and signal the adapter to move previously-
                    # emitted prose into the reasoning buffer.
                    if not self._saw_open_tag and not self._signalled_implicit:
                        channel = "thinking"
                        out.append(("close_unmatched", ""))
                        self._signalled_implicit = True
                else:
                    m = None
                    toggles_state = False
            if m is None:
                safe_end = self._safe_emit_end(self._buf)
                if safe_end > 0:
                    out.append((channel, self._buf[:safe_end]))
                    self._buf = self._buf[safe_end:]
                break
            if m.start() > 0:
                out.append((channel, self._buf[: m.start()]))
            if toggles_state:
                self._in_thinking = not self._in_thinking
            self._buf = self._buf[m.end() :]
        return out

    def flush(self) -> list[tuple[str, str]]:
        """Emit any held-back tail. Call at end of message stream."""
        if not self._buf:
            return []
        channel = "thinking" if self._in_thinking else "prose"
        out = [(channel, self._buf)]
        self._buf = ""
        return out

    @staticmethod
    def _safe_emit_end(buf: str) -> int:
        """Index up to which it's safe to flush without splitting a marker.

        - An XML-style ``<think>`` open/close needs only a short hold
          window (``_PARTIAL_TAG_HOLD``): a ``<`` followed by plenty of
          text is just literal prose, safe to emit.
        - A ``【`` may begin an inline citation marker
          (``【cite_source text="…" source="…"】``) or a gpt-oss Harmony
          marker. Those run far longer than the short window — a
          citation marker's quoted ``text=`` can be hundreds of chars.
          Hold the ENTIRE tail from an unclosed ``【`` until its
          closing ``】`` arrives, so the whole marker reaches
          ``_CITATION_MARKER_RE`` in one piece and gets stripped
          instead of streaming out half-formed.
        """
        cap = len(buf)
        open_brk = buf.rfind("【")
        if open_brk >= 0 and "】" not in buf[open_brk:]:
            cap = open_brk
        last_lt = buf.rfind("<")
        if last_lt >= 0 and len(buf) - last_lt <= _PARTIAL_TAG_HOLD:
            cap = min(cap, last_lt)
        return cap


CLIENT_SIDE_TOOL_PREFIX = "client:"

WORKSPACE_MCP_TOOL_PREFIX = "mcp:"


_WORKSPACE_NATIVE_FUNCTIONS: frozenset[str] = frozenset(
    {
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
    }
)


_WORKSPACE_ARTIFACT_TYPES: frozenset[str] = frozenset(
    {"text", "table", "chart", "snowflake_query", "snowflake_python", "html"}
)


_ArtifactWireType = Literal[
    "text", "table", "chart", "snowflake_query", "snowflake_python", "html"
]


def _resolve_artifact_wire_type(raw_type: str) -> _ArtifactWireType:
    if raw_type == "markdown":
        return "text"
    if raw_type in _WORKSPACE_ARTIFACT_TYPES:
        return cast(_ArtifactWireType, raw_type)
    return "text"


def _resolve_artifact_table_content(
    payload: dict[str, Any], raw_content: Any
) -> str | list[dict[str, Any]]:
    cols = payload.get("columns") or []
    rows = payload.get("rows") or []
    if cols and rows:
        return [
            {col: row[i] if i < len(row) else None for i, col in enumerate(cols)}
            for row in rows
        ]
    if isinstance(raw_content, list):
        return raw_content
    return str(raw_content) if raw_content is not None else ""


def _build_artifact(payload: dict[str, Any]) -> ClientArtifact:
    """Coerce a tool's artifact dict into the wire ``ClientArtifact`` shape."""
    wire_type = _resolve_artifact_wire_type(str(payload.get("type") or "text").lower())
    name = str(payload.get("name") or "")
    description = str(payload.get("description") or "")
    uuid = str(payload.get("uuid") or "")
    raw_content = payload.get("content")

    content: str | list[dict[str, Any]]
    chart_params: dict[str, Any] | None = None

    if wire_type == "table":
        content = _resolve_artifact_table_content(payload, raw_content)
    elif wire_type == "chart":
        plotly = payload.get("plotly")
        if isinstance(plotly, dict):
            chart_params = plotly
            content = ""
        else:
            content = str(raw_content) if raw_content is not None else ""
    elif isinstance(raw_content, (str, list)):
        content = raw_content
    elif raw_content is None:
        content = ""
    else:
        content = str(raw_content)

    return ClientArtifact(
        type=wire_type,
        name=name,
        description=description,
        uuid=uuid,
        content=content,
        chart_params=chart_params,
    )


def _flatten_reasoning(raw: Any) -> str:
    """Normalise ``additional_kwargs.reasoning_content`` to a flat string."""
    if not raw:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        return "".join(
            b.get("text", "") if isinstance(b, dict) else str(b) for b in raw
        )
    return str(raw)


def _split_thinking(text: str) -> tuple[list[str], str]:
    """Split text into ``(thinking_blocks, remaining_prose)``."""
    thinking_blocks: list[str] = []
    parts: list[str] = []
    last_end = 0
    for m in _THINKING_RE.finditer(text):
        parts.append(text[last_end : m.start()])
        body = m.group("body").strip()
        if body:
            thinking_blocks.append(body)
        last_end = m.end()
    parts.append(text[last_end:])
    remaining = "".join(parts)
    # Strip inline citation markers and collapse the whitespace they
    # leave behind (often a ``" ."`` sequence at the end of a sentence).
    remaining = _CITATION_MARKER_RE.sub("", remaining)
    remaining = re.sub(r"[ \t]+([.,;:!?])", r"\1", remaining)
    return thinking_blocks, remaining


def _coerce_status_event_type(
    value: Any,
) -> Literal["INFO", "SUCCESS", "WARNING", "ERROR"]:
    """Normalise to ``{INFO, WARNING, ERROR}`` for plugin-emitted steps."""
    raw = str(value or "INFO").upper()
    if raw == "WARNING":
        return "WARNING"
    if raw == "ERROR":
        return "ERROR"
    return "INFO"


def _coerce_status_details(
    raw: Any,
) -> list[dict[str, Any] | str] | None:
    """Workspace expects ``details`` as a list, never a flat dict."""
    if raw is None:
        return None
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, (dict, str))]
    if isinstance(raw, dict):
        # Drop empty mappings so Workspace doesn't render an empty card.
        return [raw] if raw else None
    if isinstance(raw, str):
        return [raw]
    return [str(raw)]


def _build_function_call(
    *,
    server_id: str,
    function: str,
    input_arguments: dict[str, Any],
    call_id: str,
) -> FunctionCallSSE:
    """Wrap a tool call in the right ``FunctionCallSSEData`` shape."""
    if function in _WORKSPACE_NATIVE_FUNCTIONS:
        # ``function`` matched _WORKSPACE_NATIVE_FUNCTIONS which is a
        # 1:1 mirror of the FunctionName Literal — safe to narrow.
        from openbb_agent_server.protocol.schemas import FunctionName

        data = FunctionCallSSEData(
            function=cast(FunctionName, function),
            input_arguments=dict(input_arguments),
            extra_state={"call_id": call_id} if call_id else None,
        )
    else:
        data = FunctionCallSSEData(
            function="execute_agent_tool",
            input_arguments={
                "server_id": server_id,
                "name": function,
                "arguments": dict(input_arguments),
            },
            extra_state={"call_id": call_id} if call_id else None,
        )
    return FunctionCallSSE(data=data)


def _extract_text(content: Any) -> str:
    """Pull user-visible text out of any LangChain ``AIMessage.content`` shape."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                btype = block.get("type")
                if btype == "text":
                    raw = block.get("text") or ""
                    if isinstance(raw, str):
                        parts.append(raw)
                elif btype is None and isinstance(block.get("text"), str):
                    parts.append(block["text"])
        return "".join(parts)
    return str(content) if content else ""


class DeepAgentEventAdapter:
    """Translate deepagents stream events to OpenBB SSE events."""

    def __init__(self, *, client_tool_names: frozenset[str] = frozenset()) -> None:
        self._client_tool_names = client_tool_names
        # Per-AIMessage streaming splitter. Created when a new message_id
        # arrives, flushed when the message ends.
        self._cur_id: str | None = None
        self._splitter: _ThinkingStreamSplitter | None = None
        # ``_reasoning_buf`` accumulates the CURRENT reasoning segment.
        # A segment starts at the beginning of the run (or right after
        # a tool dispatch fires) and ends when the next tool call
        # fires. At that point the segment is flushed as ONE
        # StatusUpdateSSE row and the buffer resets.
        #
        # ``_prose_buf`` accumulates plain prose (not <thinking>
        # content) since the last tool dispatch. It's ambiguous mid-
        # stream — prose that comes between two tool calls is a
        # reasoning preface; prose that comes after the LAST tool
        # call (or in a run with no tools at all) is the final
        # answer. We can't distinguish until either the next tool
        # call fires (→ flush as reasoning) or the stream ends
        # (→ flush as MessageChunk).
        self._reasoning_buf: list[str] = []
        self._prose_buf: list[str] = []
        # Artifacts are collected from server-side ``emit_*_artifact``
        # calls during the stream and emitted ALL AT ONCE after the
        # final-answer MessageChunk drains. Emitting them inline
        # interleaves them with reasoning rows in a way the
        # Workspace UI doesn't render cleanly; batching them at the
        # tail puts every artifact card together below the final
        # answer.
        self._artifacts: list[SSEEvent] = []
        self._citations: list[dict[str, Any]] = []
        self._citation_keys: set[tuple[str, str]] = set()

    def _emit_splits(self, splits: list[tuple[str, str]]) -> list[SSEEvent]:
        """Convert ``(channel, text)`` pairs into pending buffers.

        ``<thinking>``-channel text always flows into the reasoning
        segment. Prose text flows into ``_prose_buf`` — routing
        (reasoning vs final answer) is decided when a tool dispatch
        fires or the stream ends, NOT per AIMessage.

        A ``("close_unmatched", "")`` signal from the splitter means
        the upstream NVIDIA langchain adapter stripped an opening
        ``<think>`` tag but preserved its close — everything we
        emitted so far as prose was actually reasoning. Move
        ``_prose_buf`` into ``_reasoning_buf`` retroactively.
        """
        for channel, text in splits:
            if channel == "close_unmatched":
                if self._prose_buf:
                    self._reasoning_buf.extend(self._prose_buf)
                    self._prose_buf = []
                continue
            if not text:
                continue
            if channel == "thinking":
                self._reasoning_buf.append(text)
                continue
            prose = _CITATION_MARKER_RE.sub("", text)
            if not prose:
                continue
            self._prose_buf.append(prose)
        return []

    def _flush_reasoning_segment(self) -> list[SSEEvent]:
        """Emit the current reasoning segment (thinking + buffered prose)
        as ONE ``StatusUpdateSSE`` row. Called right BEFORE a tool
        dispatch fires — every byte of prose/thinking accumulated
        since the previous tool call becomes the row that sits
        next to the tool-call chip in the UI.
        """
        reasoning_text = "".join(self._reasoning_buf).strip()
        prose_text = "".join(self._prose_buf).strip()
        self._reasoning_buf = []
        self._prose_buf = []
        merged = "\n\n".join(s for s in (reasoning_text, prose_text) if s)
        if not merged:
            return []
        return [
            StatusUpdateSSE(data=StatusUpdateSSEData(eventType="INFO", message=merged))
        ]

    def _flush_final_answer(self) -> list[SSEEvent]:
        """Emit buffered text at end-of-stream by channel.

        Channels are authoritative:
        - ``additional_kwargs.reasoning_content`` → reasoning lane
          (one StatusUpdateSSE row).
        - plain ``content`` (with ``<thinking>`` tags stripped) →
          chat-bubble ``MessageChunkSSE`` delta.
        """
        out: list[SSEEvent] = []
        reasoning_text = "".join(self._reasoning_buf).strip()
        prose_text = "".join(self._prose_buf).strip()
        self._reasoning_buf = []
        self._prose_buf = []
        if reasoning_text:
            out.append(
                StatusUpdateSSE(
                    data=StatusUpdateSSEData(eventType="INFO", message=reasoning_text)
                )
            )
        if prose_text:
            out.append(MessageChunkSSE(data=MessageChunkSSEData(delta=prose_text)))
        return out

    def _drain_pending(self) -> list[SSEEvent]:
        """Flush splitter + buffered prose as the FINAL answer.

        Called when ``adapt()`` reaches end-of-stream OR a non-
        message ``custom`` / ``error`` event arrives that interrupts
        the normal message flow.
        """
        out: list[SSEEvent] = []
        if self._splitter is not None:
            self._emit_splits(self._splitter.flush())
            self._splitter = None
        out.extend(self._flush_final_answer())
        self._cur_id = None
        return out

    async def adapt(
        self,
        stream: AsyncIterator[dict[str, Any]],
    ) -> AsyncIterator[SSEEvent]:
        """Translate a deepagents event stream into Workspace SSE events.

        Tail order at end-of-stream:
        1. ``_drain_pending`` — flush buffered prose as the final
           ``MessageChunkSSE`` (the chat-bubble final answer).
        2. ``_drain_artifacts`` — emit every artifact that was
           buffered during the run (one ``MessageArtifactSSE`` per
           ``emit_*_artifact`` call). Artifacts ride along AFTER the
           final answer so the chat-bubble text lands first and the
           cards stack below it.
        3. ``_drain_citations`` — emit one ``CitationCollectionSSE``
           with every accumulated citation.
        """
        async for ev in stream:
            for translated in self._translate(ev):
                yield translated
        for tail in self._drain_pending():
            yield tail
        for art in self._drain_artifacts():
            yield art
        for ev in self._drain_citations():
            yield ev

    def _drain_artifacts(self) -> list[SSEEvent]:
        """Emit every buffered artifact in arrival order, then reset."""
        out = self._artifacts
        trace(logger, "adapter: draining %d buffered artifact(s)", len(out))
        self._artifacts = []
        return out

    def _drain_citations(self) -> list[SSEEvent]:
        """Emit the accumulated citations as ONE CitationCollectionSSE."""
        if not self._citations:
            return []
        citations = [Citation.model_validate(c) for c in self._citations]
        self._citations = []
        self._citation_keys = set()
        return [CitationCollectionSSE(data=CitationCollection(citations=citations))]

    def _absorb_citations(self, items: Any) -> None:
        """Accumulate citations across the run; de-dup by stable identity.

        Web / PDF citations: (origin URL, quoted snippet). The URL is
        the canonical identity for the source.

        Widget citations: the per-instance ``source_info.uuid``. The
        ``origin`` field is the integration / data-vendor name
        (``"Blackrock"``, ``"Financial Datasets"``), which is shared
        across every widget from that vendor — keying on origin would
        collapse every Blackrock widget down to one chip.
        """
        if not isinstance(items, list):
            return
        for c in items:
            if not isinstance(c, dict):
                continue
            src = c.get("source_info") or {}
            src_type = str(src.get("type") or "")
            if src_type == "widget":
                widget_uuid = str(src.get("uuid") or "") or str(
                    src.get("widget_id") or ""
                )
                # For PDF citations on a document widget, source_info
                # metadata carries ``Page: N`` — include that in the
                # dedup key so each page gets its own chip instead of
                # collapsing to a single per-widget entry.
                meta = src.get("metadata") or {}
                page = ""
                if isinstance(meta, dict):
                    page = str(meta.get("Page") or "")
                key: tuple[str, str] = (
                    "widget",
                    f"{widget_uuid}#p{page}" if page else widget_uuid,
                )
            else:
                url = str(src.get("origin") or "")
                details = c.get("details") or []
                snippet = ""
                if isinstance(details, list) and details:
                    first = details[0]
                    if isinstance(first, dict):
                        snippet = str(first.get("text") or "")
                key = (url, snippet)
            if key in self._citation_keys:
                continue
            self._citation_keys.add(key)
            self._citations.append(c)

    def _translate(self, ev: dict[str, Any]) -> list[SSEEvent]:
        kind = ev.get("type")
        ns = tuple(ev.get("ns") or ())
        data = ev.get("data") or {}

        if kind == "messages":
            return self._translate_messages(data, ns)
        if kind == "updates":
            return []
        if kind == "custom":
            custom = data or {}
            custom_type = custom.get("type")
            if custom_type == "citations":
                self._absorb_citations(custom.get("citations") or [])
                return []
            if custom_type == "artifact":
                # Buffer for end-of-stream emission, AFTER the final
                # answer MessageChunk drains. Artifacts are NOT
                # reasoning boundaries — they're collected silently
                # during the stream and rendered together once the
                # textual answer is complete. This keeps the chat-
                # bubble prose unbroken and stacks every artifact
                # card below it in arrival order.
                artifact_payload = custom.get("artifact") or {}
                try:
                    sse = MessageArtifactSSE(data=_build_artifact(artifact_payload))
                except Exception:
                    logger.exception(
                        "adapter: artifact payload failed to build "
                        "(type=%r name=%r uuid=%r)",
                        artifact_payload.get("type"),
                        artifact_payload.get("name"),
                        artifact_payload.get("uuid"),
                    )
                    return []
                self._artifacts.append(sse)
                trace(
                    logger,
                    "adapter: buffered artifact (type=%r name=%r uuid=%r) — "
                    "total buffered=%d",
                    artifact_payload.get("type"),
                    artifact_payload.get("name"),
                    artifact_payload.get("uuid"),
                    len(self._artifacts),
                )
                return []
            # Other customs (step, function_call) ARE boundary
            # events — prose accumulated up to here is reasoning
            # preceding the boundary, not final answer. Flush as a
            # reasoning segment, then emit the custom.
            if self._splitter is not None:
                self._emit_splits(self._splitter.flush())
                self._splitter = _ThinkingStreamSplitter()
            out = self._flush_reasoning_segment()
            out.extend(self._translate_custom(custom))
            return out
        if kind == "error":
            out = self._drain_pending()
            out.append(
                StatusUpdateSSE(
                    data=StatusUpdateSSEData(
                        eventType="ERROR",
                        message=str(data.get("message", "agent error")),
                        details=_coerce_status_details({"ns": list(ns), **data}),
                    )
                )
            )
            return out
        return []

    def _build_function_call_from_tool_call(
        self, tc: Any, ns: tuple[str, ...]
    ) -> SSEEvent | None:
        """Map one ``message.tool_calls[i]`` entry to a ``FunctionCallSSE``."""
        if not isinstance(tc, dict):
            return None
        raw_name = str(tc.get("name") or "")
        if not raw_name:
            return None
        args = tc.get("args") if isinstance(tc.get("args"), dict) else {}
        call_id = str(tc.get("id") or "")
        if (
            raw_name in _WORKSPACE_NATIVE_FUNCTIONS
            and raw_name not in self._client_tool_names
        ):
            return None
        if raw_name.startswith(WORKSPACE_MCP_TOOL_PREFIX):
            rest = raw_name[len(WORKSPACE_MCP_TOOL_PREFIX) :]
            if ":" in rest:
                server_id, _, function = rest.partition(":")
            else:
                server_id = ns[0] if ns else "agent"
                function = rest
        elif raw_name.startswith(CLIENT_SIDE_TOOL_PREFIX):
            server_id = ns[0] if ns else "agent"
            function = raw_name[len(CLIENT_SIDE_TOOL_PREFIX) :]
        elif raw_name in self._client_tool_names:
            server_id = "agent"
            function = raw_name
        else:
            # Server-side tool — executes inline inside the agent loop; the
            # ToolMessage result feeds back to the model. Do NOT emit a
            # FunctionCallSSE: Workspace would try to re-execute the tool
            # remotely and the model would never see a real result.
            return None
        return _build_function_call(
            server_id=server_id,
            function=function,
            input_arguments=dict(args),
            call_id=call_id,
        )

    def _translate_messages(
        self,
        data: dict[str, Any],
        ns: tuple[str, ...],
    ) -> list[SSEEvent]:
        out: list[SSEEvent] = []
        message = data.get("message") or {}
        message_id = str(message.get("id") or "")
        text = _extract_text(message.get("content"))
        tool_calls = message.get("tool_calls") or []

        deferred_fn_calls: list[SSEEvent] = []
        for tc in tool_calls:
            fc = self._build_function_call_from_tool_call(tc, ns)
            if fc is not None:
                deferred_fn_calls.append(fc)
        # ``has_tool_call`` is the reasoning-segment boundary. ANY tool
        # call closes the current reasoning row — including server-side
        # tools like ``emit_table_artifact`` that don't dispatch a
        # FunctionCallSSE. Without this, the prose preceding a
        # server-side tool keeps accumulating in ``_prose_buf`` and
        # eventually leaks out as the final-answer MessageChunk at
        # stream end — exactly the "planning text shown as final
        # answer" bug.
        has_tool_call = bool(tool_calls)

        # New message id: rotate the splitter so partial-tag state
        # doesn't bleed across messages. Buffered prose / reasoning
        # SURVIVE the rotation — they only flush on tool dispatch or
        # stream end.
        if message_id and message_id != self._cur_id:
            if self._splitter is not None:
                self._emit_splits(self._splitter.flush())
            self._cur_id = message_id
            self._splitter = _ThinkingStreamSplitter()
        elif not message_id and self._splitter is None:
            # Boundary-less event before any normal message — make a
            # disposable splitter so partial tags split cleanly.
            self._splitter = _ThinkingStreamSplitter()

        # ``additional_kwargs.reasoning_content`` and inline
        # ``<think>`` tags are TWO sources of the same data when the
        # langchain NVIDIA adapter is in use. The library populates
        # ``reasoning_content`` with the last fragment of each
        # thinking block AND leaves the tags in ``content`` — so
        # appending both would double-count that last fragment in
        # the reasoning row.
        #
        # Dedup: when ``content`` still carries the ``</think>``
        # closing tag, trust the splitter (which captures the full
        # thinking text via the ``close_unmatched`` path) and skip
        # the direct ``reasoning_content`` append. When the tag is
        # absent — the future langchain NVIDIA behavior flagged by
        # ``chat_models.py:669`` — ``reasoning_content`` becomes
        # the canonical source and we append it.
        kw = message.get("additional_kwargs") or {}
        reasoning_delta = _flatten_reasoning(
            kw.get("reasoning_content") or kw.get("reasoning") or ""
        )
        if reasoning_delta and not (text and _THINKING_CLOSE_RE.search(text)):
            self._reasoning_buf.append(reasoning_delta)

        # Feed prose through the splitter — it accumulates into
        # _reasoning_buf or _prose_buf depending on the channel.
        if text and self._splitter is not None:
            self._emit_splits(self._splitter.feed(text))

        # Any tool call closes the current reasoning segment. Flush
        # accumulated thinking + prose as ONE StatusUpdateSSE row.
        # Then, only if the tool dispatches client-side, append the
        # FunctionCallSSE — server-side tools (emit_*_artifact,
        # read_widget_data, etc.) run inline inside the agent loop and
        # don't need an SSE dispatch.
        if has_tool_call:
            if self._splitter is not None:
                self._emit_splits(self._splitter.flush())
                self._splitter = _ThinkingStreamSplitter()
            out.extend(self._flush_reasoning_segment())
            if deferred_fn_calls:
                out.extend(deferred_fn_calls)
        return out

    def _translate_custom(self, data: dict[str, Any]) -> list[SSEEvent]:
        # Plugins emit dicts via ``get_stream_writer()`` shaped like one of
        # the OpenBB SSE event variants — we re-emit if so, else wrap as INFO.
        kind = data.get("type")
        if kind == "chunk":
            return [
                MessageChunkSSE(
                    data=MessageChunkSSEData(delta=str(data.get("content", "")))
                )
            ]
        if kind == "step":
            return [
                StatusUpdateSSE(
                    data=StatusUpdateSSEData(
                        eventType=_coerce_status_event_type(data.get("event_type")),
                        message=str(data.get("message", "")),
                        details=_coerce_status_details(data.get("details")),
                    )
                )
            ]
        if kind == "artifact":
            # Inline emission — Workspace renders artifact cards
            # interleaved with the model's prose, same as the OpenBB
            # Copilot UX. Reasoning is buffered into a single row per
            # AIMessage and emitted before this artifact (the flush
            # happens at the tool-dispatch / message-end boundary), so
            # we don't tear the reasoning container.
            return [
                MessageArtifactSSE(data=_build_artifact(data.get("artifact") or {}))
            ]
        if kind == "function_call":
            return [
                _build_function_call(
                    server_id=str(data.get("server_id", "agent")),
                    function=str(data["tool_name"]),
                    input_arguments=data.get("parameters") or {},
                    call_id=str(data.get("call_id", "")),
                )
            ]
        return [
            StatusUpdateSSE(
                data=StatusUpdateSSEData(
                    eventType="INFO",
                    message=str(data.get("message", "")),
                    details=_coerce_status_details(data),
                )
            )
        ]
