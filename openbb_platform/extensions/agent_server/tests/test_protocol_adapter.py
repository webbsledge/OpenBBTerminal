"""DeepAgentEventAdapter translation tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from openbb_agent_server.protocol.adapter import (
    CLIENT_SIDE_TOOL_PREFIX,
    DeepAgentEventAdapter,
)
from openbb_agent_server.protocol.schemas import (
    CitationCollectionSSE,
    FunctionCallSSE,
    MessageArtifactSSE,
    MessageChunkSSE,
    StatusUpdateSSE,
)


async def _drive(
    adapter: DeepAgentEventAdapter, raw: list[dict[str, Any]]
) -> list[Any]:
    async def gen() -> AsyncIterator[dict[str, Any]]:
        for r in raw:
            yield r

    out: list[Any] = []
    async for ev in adapter.adapt(gen()):
        out.append(ev)
    return out


@pytest.mark.asyncio
async def test_natural_completion_does_not_emit_turn_complete_marker() -> None:
    """The adapter must NOT emit a trailing ``Turn complete.`` ``StatusUpdateSSE``. Workspace closes the SSE stream when the generator exits; an extra synthetic ``SUCCESS`` event clutters the reasoning lane."""
    adapter = DeepAgentEventAdapter()

    async def gen() -> AsyncIterator[dict[str, Any]]:
        yield {"type": "messages", "ns": [], "data": {"message": {"content": "hi"}}}

    out: list[Any] = []
    async for ev in adapter.adapt(gen()):
        out.append(ev)
    assert not any(
        isinstance(ev, StatusUpdateSSE)
        and getattr(ev.data, "message", None) == "Turn complete."
        for ev in out
    )


@pytest.mark.asyncio
async def test_text_chunk_becomes_message_chunk_sse() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [{"type": "messages", "ns": [], "data": {"message": {"content": "hi"}}}],
    )
    assert len(out) == 1
    assert isinstance(out[0], MessageChunkSSE)
    assert out[0].data.delta == "hi"


@pytest.mark.asyncio
async def test_empty_text_chunk_is_dropped() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [{"type": "messages", "data": {"message": {"content": ""}}}],
    )
    assert out == []


@pytest.mark.asyncio
async def test_client_side_tool_call_routes_through_execute_agent_tool() -> None:
    """Tool names outside Workspace's closed enum must be wrapped as ``execute_agent_tool`` with the real name nested in ``input_arguments`` — Workspace's UI rejects ``function`` values it doesn't recognise."""
    raw = [
        {
            "type": "messages",
            "ns": ["main"],
            "data": {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "name": f"{CLIENT_SIDE_TOOL_PREFIX}open_widget",
                            "args": {"widget_id": "w1"},
                            "id": "call-1",
                        }
                    ],
                }
            },
        }
    ]
    out = await _drive(DeepAgentEventAdapter(), raw)
    assert len(out) == 1
    fc = out[0]
    assert isinstance(fc, FunctionCallSSE)
    assert fc.data.function == "execute_agent_tool"
    assert fc.data.input_arguments == {
        "server_id": "main",
        "name": "open_widget",
        "arguments": {"widget_id": "w1"},
    }
    assert fc.data.extra_state == {"call_id": "call-1"}


@pytest.mark.asyncio
async def test_native_workspace_function_routes_directly() -> None:
    """Names that ARE in Workspace's closed enum (e.g. ``get_widget_data``) pass through unchanged with their args at the top of input_arguments."""
    raw = [
        {
            "type": "messages",
            "ns": [],
            "data": {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "name": "get_widget_data",
                            "args": {"widget_id": "w-1"},
                            "id": "c-2",
                        }
                    ],
                }
            },
        }
    ]
    out = await _drive(
        DeepAgentEventAdapter(client_tool_names=frozenset({"get_widget_data"})),
        raw,
    )
    assert isinstance(out[0], FunctionCallSSE)
    assert out[0].data.function == "get_widget_data"
    assert out[0].data.input_arguments == {"widget_id": "w-1"}


@pytest.mark.asyncio
async def test_explicit_client_tool_set_routes_to_function_call() -> None:
    out = await _drive(
        DeepAgentEventAdapter(client_tool_names=frozenset({"render_chart"})),
        [
            {
                "type": "messages",
                "data": {
                    "message": {
                        "content": "",
                        "tool_calls": [{"name": "render_chart", "args": {}, "id": "x"}],
                    }
                },
            }
        ],
    )
    assert len(out) == 1
    assert isinstance(out[0], FunctionCallSSE)
    # Unknown to Workspace → wrapped as execute_agent_tool.
    assert out[0].data.function == "execute_agent_tool"
    assert out[0].data.input_arguments["name"] == "render_chart"


@pytest.mark.asyncio
async def test_server_side_tool_call_emits_no_function_call() -> None:
    """Plain tool calls — no ``client:`` / ``mcp:`` prefix, not in ``client_tool_names`` — execute inline inside the agent loop. Emitting a ``FunctionCallSSE`` here would cause Workspace to re-run the tool remotely and the model would never see a real result, so it must be suppressed."""
    out = await _drive(
        DeepAgentEventAdapter(client_tool_names=frozenset()),
        [
            {
                "type": "messages",
                "data": {
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "name": "list_widget_data",
                                "args": {},
                                "id": "c-1",
                            }
                        ],
                    }
                },
            }
        ],
    )
    assert not any(isinstance(ev, FunctionCallSSE) for ev in out)


@pytest.mark.asyncio
async def test_reasoning_content_coalesces_into_one_step_per_aimessage() -> None:
    """All ``reasoning_content`` deltas within one AIMessage buffer into ONE ``StatusUpdateSSE`` row that fires when the message closes (tool dispatch / id transition / end of stream)."""
    from openbb_agent_server.protocol.schemas import StatusUpdateSSE

    raw = [
        {
            "type": "messages",
            "ns": [],
            "data": {
                "message": {
                    "id": "m-r",
                    "content": "",
                    "additional_kwargs": {"reasoning_content": "Let"},
                }
            },
        },
        {
            "type": "messages",
            "ns": [],
            "data": {
                "message": {
                    "id": "m-r",
                    "content": "",
                    "additional_kwargs": {"reasoning_content": " me think."},
                }
            },
        },
    ]
    out = await _drive(DeepAgentEventAdapter(), raw)
    status_msgs = [ev.data.message for ev in out if isinstance(ev, StatusUpdateSSE)]
    assert status_msgs == ["Let me think."]


@pytest.mark.asyncio
async def test_prose_during_tool_call_routes_to_reasoning() -> None:
    """Prose alongside tool_calls in the same AIMessage is an intermediate reasoning preface, NOT the final answer — it surfaces as a ``StatusUpdateSSE`` row inside the "Step-by-step reasoning" container, not as a message bubble in the answer area."""
    from openbb_agent_server.protocol.schemas import (
        MessageChunkSSE,
        StatusUpdateSSE,
    )

    raw = [
        {
            "type": "messages",
            "ns": [],
            "data": {
                "message": {
                    "id": "m-t",
                    "content": "I'll fetch the balance sheet.",
                    "tool_calls": [
                        {"name": "list_widget_data", "args": {}, "id": "c-1"}
                    ],
                }
            },
        }
    ]
    out = await _drive(DeepAgentEventAdapter(), raw)
    assert any(
        isinstance(ev, StatusUpdateSSE) and "balance sheet" in ev.data.message
        for ev in out
    )
    assert not any(
        isinstance(ev, MessageChunkSSE) and "balance sheet" in ev.data.delta
        for ev in out
    )


@pytest.mark.asyncio
async def test_workspace_mcp_tool_carries_server_id_through_arguments() -> None:
    """``mcp:<server>:<fn>`` round-trips both the server id and the function name through ``execute_agent_tool``'s input_arguments."""
    raw = [
        {
            "type": "messages",
            "ns": ["graph"],
            "data": {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "name": "mcp:wkspc:get_quote",
                            "args": {"ticker": "AAPL"},
                            "id": "c-9",
                        }
                    ],
                }
            },
        }
    ]
    out = await _drive(DeepAgentEventAdapter(), raw)
    assert isinstance(out[0], FunctionCallSSE)
    assert out[0].data.function == "execute_agent_tool"
    assert out[0].data.input_arguments == {
        "server_id": "wkspc",
        "name": "get_quote",
        "arguments": {"ticker": "AAPL"},
    }


@pytest.mark.asyncio
async def test_messages_event_extracts_text_from_block_list_content() -> None:
    """Anthropic / Vertex / structured-output providers ship content as ``[{"type": "text", "text": "..."}, ...]``. The adapter must surface only the text blocks — otherwise the UI gets empty bubbles."""
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "messages",
                "ns": [],
                "data": {
                    "message": {
                        "content": [
                            {"type": "thinking", "thinking": "internal scratch"},
                            {"type": "text", "text": "Hello "},
                            {"type": "text", "text": "world!"},
                        ],
                        "tool_calls": [],
                    },
                },
            },
        ],
    )
    chunks = [e for e in out if isinstance(e, MessageChunkSSE)]
    assert chunks, "block-list text content must surface as a chunk"
    assert chunks[0].data.delta == "Hello world!"


@pytest.mark.asyncio
async def test_messages_event_string_content_still_works() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "messages",
                "ns": [],
                "data": {"message": {"content": "plain string", "tool_calls": []}},
            },
        ],
    )
    chunks = [e for e in out if isinstance(e, MessageChunkSSE)]
    assert chunks[0].data.delta == "plain string"


@pytest.mark.asyncio
async def test_messages_event_skips_thinking_and_tool_use_blocks() -> None:
    """Reasoning / tool_use blocks are model-internal — never broadcast."""
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "messages",
                "ns": [],
                "data": {
                    "message": {
                        "content": [
                            {"type": "thinking", "thinking": "secret reasoning"},
                            {"type": "tool_use", "id": "x", "name": "y", "input": {}},
                            {"type": "redacted_thinking", "data": "..."},
                        ],
                        "tool_calls": [],
                    },
                },
            },
        ],
    )
    chunks = [e for e in out if isinstance(e, MessageChunkSSE)]
    assert chunks == []


@pytest.mark.asyncio
async def test_messages_event_empty_content_emits_nothing() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "messages",
                "ns": [],
                "data": {"message": {"content": "", "tool_calls": []}},
            },
            {
                "type": "messages",
                "ns": [],
                "data": {"message": {"content": None, "tool_calls": []}},
            },
            {
                "type": "messages",
                "ns": [],
                "data": {"message": {"content": [], "tool_calls": []}},
            },
        ],
    )
    chunks = [e for e in out if isinstance(e, MessageChunkSSE)]
    assert chunks == []


@pytest.mark.asyncio
async def test_extract_text_handles_string_blocks_in_list() -> None:
    """Older LangChain shapes pack content as ``["chunk-a", "chunk-b"]``."""
    from openbb_agent_server.protocol.adapter import _extract_text

    assert _extract_text(["a", "b", "c"]) == "abc"


@pytest.mark.asyncio
async def test_updates_event_is_dropped_never_reaches_ui() -> None:
    """``updates`` events carry middleware lifecycle steps — never broadcast."""
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "updates",
                "ns": [],
                "data": {"ModelCallLimitMiddleware.before_model": {}},
            },
            {
                "type": "updates",
                "ns": ["tools:abc"],
                "data": {"_MemoryWriterMiddleware.after_agent": {}},
            },
        ],
    )
    assert out == []


@pytest.mark.asyncio
async def test_custom_chunk_passthrough() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [{"type": "custom", "data": {"type": "chunk", "content": "passthrough"}}],
    )
    assert isinstance(out[0], MessageChunkSSE)
    assert out[0].data.delta == "passthrough"


@pytest.mark.asyncio
async def test_artifacts_drain_after_final_answer() -> None:
    """Artifacts buffer during the stream and drain AFTER the final
    ``MessageChunkSSE`` so chat-bubble prose lands first and the
    artifact cards stack below it in arrival order.
    """
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "artifact",
                    "artifact": {
                        "type": "text",
                        "uuid": "art-1",
                        "name": "summary",
                        "description": "",
                        "content": "hello",
                    },
                },
            },
            {
                "type": "messages",
                "ns": [],
                "data": {"message": {"id": "m-final", "content": "All done."}},
            },
        ],
    )
    types = [type(ev).__name__ for ev in out]
    assert types.index("MessageChunkSSE") < types.index("MessageArtifactSSE")


@pytest.mark.asyncio
async def test_custom_artifact_table_is_flattened_to_records() -> None:
    """Workspace tables consume a list-of-records ``content`` shape; adapter converts ``columns``/``rows`` shorthand to that shape."""
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "artifact",
                    "artifact": {
                        "type": "table",
                        "uuid": "u1",
                        "name": "Top 5",
                        "description": "five rows",
                        "columns": ["a", "b"],
                        "rows": [[1, 2], [3, 4]],
                    },
                },
            }
        ],
    )
    a = out[0]
    assert isinstance(a, MessageArtifactSSE)
    assert a.data.type == "table"
    assert a.data.uuid == "u1"
    assert a.data.content == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


@pytest.mark.asyncio
async def test_custom_artifact_html_passthrough() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "artifact",
                    "artifact": {
                        "type": "html",
                        "uuid": "h1",
                        "name": "Report",
                        "description": "...",
                        "content": "<h1>hi</h1>",
                    },
                },
            }
        ],
    )
    a = out[0]
    assert isinstance(a, MessageArtifactSSE)
    assert a.data.type == "html"
    assert a.data.content == "<h1>hi</h1>"


@pytest.mark.asyncio
async def test_custom_artifact_markdown_is_coerced_to_text() -> None:
    """Workspace's wire spec doesn't list ``markdown`` as an artifact type; the adapter coerces it to ``text`` so the body still renders."""
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "artifact",
                    "artifact": {
                        "type": "markdown",
                        "uuid": "m1",
                        "name": "Notes",
                        "description": "...",
                        "content": "# Hi",
                    },
                },
            }
        ],
    )
    a = out[0]
    assert isinstance(a, MessageArtifactSSE)
    assert a.data.type == "text"
    assert a.data.content == "# Hi"


@pytest.mark.asyncio
async def test_custom_artifact_chart_routes_plotly_to_chart_params() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "artifact",
                    "artifact": {
                        "type": "chart",
                        "uuid": "c1",
                        "name": "Trend",
                        "description": "...",
                        "plotly": {"data": [{"x": [1], "y": [2]}]},
                    },
                },
            }
        ],
    )
    a = out[0]
    assert isinstance(a, MessageArtifactSSE)
    assert a.data.type == "chart"
    assert a.data.chart_params == {"data": [{"x": [1], "y": [2]}]}


@pytest.mark.asyncio
async def test_error_event_becomes_error_status() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [{"type": "error", "data": {"message": "kaboom"}}],
    )
    s = out[0]
    assert isinstance(s, StatusUpdateSSE)
    assert s.data.eventType == "ERROR"
    assert "kaboom" in s.data.message


@pytest.mark.asyncio
async def test_unknown_event_kind_is_silently_dropped() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [{"type": "future-thing", "data": {"x": 1}}],
    )
    assert out == []


@pytest.mark.asyncio
async def test_custom_step_passthrough() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "step",
                    "event_type": "WARNING",
                    "message": "hot path",
                    "details": {"x": 1},
                },
            }
        ],
    )
    s = out[0]
    assert isinstance(s, StatusUpdateSSE)
    assert s.data.eventType == "WARNING"
    assert s.data.message == "hot path"
    # ``details`` is wrapped in a list per the wire spec.
    assert s.data.details == [{"x": 1}]


@pytest.mark.asyncio
async def test_custom_step_success_is_coerced_to_info() -> None:
    """``SUCCESS`` is not a valid wire eventType — the adapter folds it into ``INFO`` so existing emitter sites keep working."""
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "step",
                    "event_type": "SUCCESS",
                    "message": "done",
                },
            }
        ],
    )
    s = out[0]
    assert isinstance(s, StatusUpdateSSE)
    assert s.data.eventType == "INFO"


@pytest.mark.asyncio
async def test_custom_function_call_passthrough() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "function_call",
                    "server_id": "wkspc",
                    "tool_name": "open_widget",
                    "parameters": {"id": "w1"},
                    "call_id": "c-9",
                },
            }
        ],
    )
    fc = out[0]
    assert isinstance(fc, FunctionCallSSE)
    assert fc.data.function == "execute_agent_tool"
    assert fc.data.input_arguments == {
        "server_id": "wkspc",
        "name": "open_widget",
        "arguments": {"id": "w1"},
    }
    assert fc.data.extra_state == {"call_id": "c-9"}


@pytest.mark.asyncio
async def test_custom_citations_passthrough() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "citations",
                    "citations": [
                        {
                            "id": "c-1",
                            "source_info": {
                                "type": "web",
                                "name": "Reuters",
                                "origin": "https://x.example",
                            },
                            "details": [{"text": "Quote"}],
                        }
                    ],
                },
            }
        ],
    )
    cc = out[0]
    assert isinstance(cc, CitationCollectionSSE)
    assert len(cc.data.citations) == 1
    assert cc.data.citations[0].id == "c-1"
    assert cc.data.citations[0].source_info.origin == "https://x.example"


@pytest.mark.asyncio
async def test_custom_unknown_kind_falls_back_to_info_status() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [{"type": "custom", "data": {"type": "weird", "message": "hello"}}],
    )
    s = out[0]
    assert isinstance(s, StatusUpdateSSE)
    assert s.data.eventType == "INFO"
    assert s.data.message == "hello"


@pytest.mark.asyncio
async def test_updates_event_with_namespace_still_dropped() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [{"type": "updates", "ns": ["tools:abc"], "data": {"agent": {}}}],
    )
    assert out == []


@pytest.mark.asyncio
async def test_updates_event_with_empty_data_still_dropped() -> None:
    out = await _drive(
        DeepAgentEventAdapter(),
        [{"type": "updates", "ns": [], "data": {}}],
    )
    assert out == []


def test_resolve_artifact_wire_type_maps_markdown_to_text() -> None:
    from openbb_agent_server.protocol.adapter import _resolve_artifact_wire_type

    assert _resolve_artifact_wire_type("markdown") == "text"


def test_resolve_artifact_wire_type_unknown_type_falls_back_to_text() -> None:
    from openbb_agent_server.protocol.adapter import _resolve_artifact_wire_type

    assert _resolve_artifact_wire_type("nope-foo") == "text"


def test_resolve_artifact_table_content_falls_back_to_raw_list() -> None:
    """When ``columns``/``rows`` are absent, fall back to ``raw_content``."""
    from openbb_agent_server.protocol.adapter import (
        _resolve_artifact_table_content,
    )

    out = _resolve_artifact_table_content({}, [{"a": 1}])
    assert out == [{"a": 1}]


def test_resolve_artifact_table_content_string_fallback_when_no_data() -> None:
    from openbb_agent_server.protocol.adapter import (
        _resolve_artifact_table_content,
    )

    out = _resolve_artifact_table_content({}, "plain text")
    assert out == "plain text"


def test_resolve_artifact_table_content_handles_none_raw_content() -> None:
    from openbb_agent_server.protocol.adapter import (
        _resolve_artifact_table_content,
    )

    out = _resolve_artifact_table_content({}, None)
    assert out == ""


def test_build_artifact_chart_with_non_dict_plotly_falls_back_to_string_content() -> (
    None
):
    """A chart artifact without a dict ``plotly`` payload stringifies content."""
    from openbb_agent_server.protocol.adapter import _build_artifact

    a = _build_artifact({"type": "chart", "plotly": "not-a-dict", "content": "x"})
    assert a.type == "chart"
    assert a.chart_params is None
    assert a.content == "x"


def test_build_artifact_html_content_passes_through() -> None:
    from openbb_agent_server.protocol.adapter import _build_artifact

    a = _build_artifact({"type": "html", "content": "<h1>hi</h1>"})
    assert a.type == "html"
    assert a.content == "<h1>hi</h1>"


def test_build_artifact_none_content_becomes_empty_string() -> None:
    from openbb_agent_server.protocol.adapter import _build_artifact

    a = _build_artifact({"type": "html"})
    assert a.content == ""


def test_build_artifact_non_string_non_list_content_stringifies() -> None:
    from openbb_agent_server.protocol.adapter import _build_artifact

    a = _build_artifact({"type": "html", "content": 42})
    assert a.content == "42"


def test_coerce_status_event_type_normalises_known_levels() -> None:
    from openbb_agent_server.protocol.adapter import _coerce_status_event_type

    assert _coerce_status_event_type("INFO") == "INFO"
    assert _coerce_status_event_type("info") == "INFO"
    assert _coerce_status_event_type("WARNING") == "WARNING"
    assert _coerce_status_event_type("ERROR") == "ERROR"
    assert _coerce_status_event_type(None) == "INFO"
    # SUCCESS not in the whitelist — folded to INFO.
    assert _coerce_status_event_type("SUCCESS") == "INFO"


def test_coerce_status_details_none_returns_none() -> None:
    from openbb_agent_server.protocol.adapter import _coerce_status_details

    assert _coerce_status_details(None) is None


def test_coerce_status_details_list_filters_non_dict_strings() -> None:
    from openbb_agent_server.protocol.adapter import _coerce_status_details

    out = _coerce_status_details([{"k": "v"}, "ok", 42, None])
    assert out == [{"k": "v"}, "ok"]


def test_coerce_status_details_empty_dict_returns_none() -> None:
    from openbb_agent_server.protocol.adapter import _coerce_status_details

    assert _coerce_status_details({}) is None


def test_coerce_status_details_dict_wraps_in_list() -> None:
    from openbb_agent_server.protocol.adapter import _coerce_status_details

    out = _coerce_status_details({"k": "v"})
    assert out == [{"k": "v"}]


def test_coerce_status_details_string_wraps_in_list() -> None:
    from openbb_agent_server.protocol.adapter import _coerce_status_details

    out = _coerce_status_details("hello")
    assert out == ["hello"]


def test_coerce_status_details_other_type_stringifies() -> None:
    from openbb_agent_server.protocol.adapter import _coerce_status_details

    out = _coerce_status_details(42)
    assert out == ["42"]


def test_split_thinking_extracts_blocks() -> None:
    from openbb_agent_server.protocol.adapter import _split_thinking

    thinking, prose = _split_thinking(
        "<think>step 1</think>The answer is 42<think>step 2</think>"
    )
    assert thinking == ["step 1", "step 2"]
    assert prose == "The answer is 42"


def test_split_thinking_skips_empty_thinking_blocks() -> None:
    from openbb_agent_server.protocol.adapter import _split_thinking

    thinking, prose = _split_thinking("<think>   </think>hello")
    assert thinking == []
    assert prose == "hello"


def test_extract_text_handles_block_with_no_type_but_text_field() -> None:
    """A content block without a ``type`` field but with ``text`` still concatenates."""
    from openbb_agent_server.protocol.adapter import _extract_text

    out = _extract_text([{"text": "hello"}, {"text": "world"}])
    assert "hello" in out
    assert "world" in out


def test_extract_text_handles_non_list_non_string() -> None:
    """Anything other than ``str`` / ``list`` falls through to ``str(...)``."""
    from openbb_agent_server.protocol.adapter import _extract_text

    assert _extract_text(42) == "42"
    assert _extract_text(None) == ""


def test_thinking_splitter_routes_blocks_to_status_then_chunk() -> None:
    """Inline ``<think>...</think>`` routes to ``StatusUpdateSSE``, the rest to ``MessageChunkSSE``."""
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter
    from openbb_agent_server.protocol.schemas import (
        MessageChunkSSE,
        StatusUpdateSSE,
    )

    adapter = DeepAgentEventAdapter()
    adapter._cur_id = "m-1"
    adapter._splitter = _ThinkingStreamSplitter()
    splits = adapter._splitter.feed("<think>step a</think>final answer")
    out = adapter._emit_splits(splits) + adapter._drain_pending()
    assert any(isinstance(e, StatusUpdateSSE) for e in out)
    assert any(isinstance(e, MessageChunkSSE) for e in out)


def test_thinking_splitter_buffers_prose_until_close() -> None:
    """Prose buffers in ``_prose_buf`` and is routed at boundary-time
    (tool dispatch → reasoning row, end-of-stream → final answer).
    """
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    adapter = DeepAgentEventAdapter()
    adapter._splitter = _ThinkingStreamSplitter()
    out = adapter._emit_splits(adapter._splitter.feed("hello world"))
    assert out == []
    assert "".join(adapter._prose_buf) == "hello world"


def test_thinking_splitter_holds_back_only_partial_tag_tail() -> None:
    """Hold-back is the minimum needed to detect a tag split across chunks. Everything before a trailing ``<`` flushes on the same tick; ``hello `` streams immediately, only the ``<thi`` tail waits for the rest of the tag."""
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    first = sp.feed("hello <thi")
    assert first == [("prose", "hello ")]
    second = sp.feed("nking>secret</thinking>tail")
    channels = [c for c, _ in second]
    assert "thinking" in channels
    assert ("prose", "tail") in second


def test_thinking_splitter_switches_channel_when_full_tag_arrives_in_one_delta() -> (
    None
):
    """When the whole ``<thinking>...</thinking>`` block lands in a single chunk, the splitter routes the body to the thinking channel and the trailing prose back to the prose channel."""
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    out = sp.feed("pre <thinking>secret</thinking>tail")
    assert out == [
        ("prose", "pre "),
        ("thinking", "secret"),
        ("prose", "tail"),
    ]


def test_thinking_splitter_does_not_hold_back_when_no_lt_in_buffer() -> None:
    """If the buffer contains no ``<``, every character flushes — no spurious buffering of plain prose."""
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    out = sp.feed("plain text with no tag")
    assert out == [("prose", "plain text with no tag")]


def test_absorb_citations_skips_non_list_input() -> None:
    adapter = DeepAgentEventAdapter()
    adapter._absorb_citations("not-a-list")
    assert adapter._citations == []


def test_absorb_citations_skips_non_dict_entries() -> None:
    adapter = DeepAgentEventAdapter()
    adapter._absorb_citations(["bad-entry", {"source_info": {"origin": "u"}}])
    assert len(adapter._citations) == 1


def test_absorb_citations_dedupes_by_url_and_snippet() -> None:
    adapter = DeepAgentEventAdapter()
    payload = {
        "source_info": {"origin": "http://x"},
        "details": [{"text": "snippet"}],
    }
    adapter._absorb_citations([payload, payload])
    assert len(adapter._citations) == 1


def test_build_function_call_skips_non_dict_tool_call() -> None:
    adapter = DeepAgentEventAdapter()
    assert adapter._build_function_call_from_tool_call("not-a-dict", ("ns",)) is None


def test_build_function_call_skips_blank_name() -> None:
    adapter = DeepAgentEventAdapter()
    assert (
        adapter._build_function_call_from_tool_call(
            {"name": "", "args": {}, "id": "x"}, ("ns",)
        )
        is None
    )


def test_build_function_call_skips_workspace_native_when_not_client_tool() -> None:
    """Workspace's built-in functions are filtered unless the agent declared them."""
    from openbb_agent_server.protocol.adapter import _WORKSPACE_NATIVE_FUNCTIONS

    natives = list(_WORKSPACE_NATIVE_FUNCTIONS)
    if not natives:
        pytest.skip("no built-in native functions to test against")
    adapter = DeepAgentEventAdapter()
    out = adapter._build_function_call_from_tool_call(
        {"name": natives[0], "args": {}, "id": "x"}, ("ns",)
    )
    assert out is None


def test_translate_messages_emits_reasoning_kwarg_as_status_update() -> None:
    """``additional_kwargs.reasoning_content`` is surfaced as an INFO step."""
    from openbb_agent_server.protocol.schemas import StatusUpdateSSE

    adapter = DeepAgentEventAdapter()
    out = adapter._translate_messages(
        {
            "message": {
                "id": "",
                "content": "",
                "tool_calls": [],
                "additional_kwargs": {"reasoning_content": "thinking step"},
            }
        },
        ns=("agent",),
    )
    out += adapter._drain_pending()
    assert any(
        isinstance(e, StatusUpdateSSE) and "thinking step" in e.data.message
        for e in out
    )


def test_translate_messages_flattens_reasoning_block_list() -> None:
    """``reasoning_content`` as a list of blocks is joined."""
    from openbb_agent_server.protocol.schemas import StatusUpdateSSE

    adapter = DeepAgentEventAdapter()
    out = adapter._translate_messages(
        {
            "message": {
                "id": "",
                "content": "",
                "tool_calls": [],
                "additional_kwargs": {
                    "reasoning_content": [
                        {"text": "step a "},
                        {"text": "step b"},
                    ]
                },
            }
        },
        ns=("agent",),
    )
    out += adapter._drain_pending()
    found = [e for e in out if isinstance(e, StatusUpdateSSE)]
    assert any("step a step b" in e.data.message for e in found)


def test_translate_error_event_emits_error_status() -> None:
    """``type=error`` events become ``StatusUpdateSSE`` ERRORs."""
    from openbb_agent_server.protocol.schemas import StatusUpdateSSE

    adapter = DeepAgentEventAdapter()
    out = adapter._translate(
        {"type": "error", "ns": ["x"], "data": {"message": "kaboom"}}
    )
    [ev] = out
    assert isinstance(ev, StatusUpdateSSE)
    assert ev.data.eventType == "ERROR"
    assert "kaboom" in ev.data.message


def test_translate_unknown_kind_returns_empty() -> None:
    adapter = DeepAgentEventAdapter()
    assert adapter._translate({"type": "what?", "ns": [], "data": {}}) == []


def test_translate_messages_id_less_with_tool_indicator_routes_to_reasoning() -> None:
    """Content alongside tool_calls in an ID-less message routes to the reasoning lane — it's an intermediate preface, not the final answer."""
    from openbb_agent_server.protocol.schemas import MessageChunkSSE, StatusUpdateSSE

    adapter = DeepAgentEventAdapter()
    out = adapter._translate_messages(
        {
            "message": {
                "id": "",
                "content": "I will call the tool now",
                "tool_calls": [{"name": "search", "args": {}, "id": "c1"}],
            }
        },
        ns=("agent",),
    )
    assert any(
        isinstance(e, StatusUpdateSSE) and "call the tool" in e.data.message
        for e in out
    )
    assert all(not isinstance(e, MessageChunkSSE) for e in out)


def test_splitter_empty_delta_returns_empty() -> None:
    """An empty delta is a no-op — the early-return guard."""
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    assert sp.feed("") == []


def test_splitter_suppresses_after_harmony_marker() -> None:
    """The first Harmony marker truncates the buffer and refuses further deltas."""
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    out = sp.feed("visible answer <|channel|>functions.tool_call")
    assert out == [("prose", "visible answer ")]
    # Every subsequent delta is dropped on the harmony-suppress guard.
    assert sp.feed("more leaked text") == []
    assert sp._harmony_suppress is True


def test_splitter_stray_close_reclassifies_preceding_prose() -> None:
    """A ``</think>`` with no matching open emits a ``close_unmatched`` marker
    and routes the preceding text to the thinking channel.
    """
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    out = sp.feed("hidden reasoning</think>visible tail")
    assert ("close_unmatched", "") in out
    assert ("thinking", "hidden reasoning") in out
    assert ("prose", "visible tail") in out
    # Single-shot: a second stray close does NOT re-emit the marker.
    out2 = sp.feed("more</think>again")
    assert ("close_unmatched", "") not in out2


def test_splitter_flush_emits_held_thinking_tail() -> None:
    """``flush`` inside an unterminated thinking block emits on the thinking
    channel.
    """
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    # Trailing ``<`` is held back as a possible partial tag.
    sp.feed("<thinking>still going <")
    flushed = sp.flush()
    assert flushed == [("thinking", "<")]
    # A second flush with an empty buffer yields nothing.
    assert sp.flush() == []


def test_splitter_safe_emit_end_holds_back_long_buffer_past_marker() -> None:
    """A ``<`` far from the buffer tail flushes everything before it."""
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    # ``<`` at index 0, buffer far longer than _PARTIAL_TAG_HOLD → flush all.
    out = sp.feed("<" + "x" * 64)
    assert out == [("prose", "<" + "x" * 64)]


def test_splitter_holds_unclosed_citation_marker_until_closed() -> None:
    """An unclosed ``【`` is held in full — even far past _PARTIAL_TAG_HOLD —
    until its ``】`` arrives, so a long ``【cite_source …】`` marker never
    streams out half-formed.
    """
    from openbb_agent_server.protocol.adapter import _ThinkingStreamSplitter

    sp = _ThinkingStreamSplitter()
    # ``【`` then far more than _PARTIAL_TAG_HOLD chars, still unclosed —
    # only the text before ``【`` is safe to emit.
    out = sp.feed('answer text 【cite_source text="' + "y" * 200)
    assert out == [("prose", "answer text ")]
    # Closing ``】`` + trailing prose arrive: the whole marker flushes
    # as one chunk so the strip regex can match it.
    [(channel, text)] = sp.feed('" source="f.pdf"】 and the rest')
    assert channel == "prose"
    assert text.startswith("【cite_source") and text.endswith(" and the rest")


def test_emit_splits_strips_inline_cite_source_marker() -> None:
    """A ``【cite_source …】`` marker emitted as text is stripped from prose
    before it reaches the chat bubble.
    """
    adapter = DeepAgentEventAdapter()
    adapter._emit_splits(
        [
            (
                "prose",
                'See page 138.【cite_source text="basket" '
                'source="iefa_prospectus.pdf" source_url=null】 Next sentence.',
            )
        ]
    )
    assert adapter._prose_buf == ["See page 138. Next sentence."]


def test_flatten_reasoning_non_string_non_list_stringifies() -> None:
    """A reasoning payload that is neither ``str`` nor ``list`` is stringified."""
    from openbb_agent_server.protocol.adapter import _flatten_reasoning

    assert _flatten_reasoning(42) == "42"


def test_emit_splits_close_unmatched_moves_prose_to_reasoning() -> None:
    """A ``close_unmatched`` signal retroactively moves buffered prose into
    the reasoning buffer.
    """
    adapter = DeepAgentEventAdapter()
    adapter._prose_buf = ["earlier prose"]
    adapter._emit_splits([("close_unmatched", "")])
    assert adapter._prose_buf == []
    assert adapter._reasoning_buf == ["earlier prose"]


def test_emit_splits_skips_empty_text_pairs() -> None:
    """An empty ``(channel, text)`` pair contributes nothing."""
    adapter = DeepAgentEventAdapter()
    adapter._emit_splits([("prose", ""), ("thinking", "")])
    assert adapter._prose_buf == []
    assert adapter._reasoning_buf == []


def test_emit_splits_prose_that_is_all_citation_marker_is_dropped() -> None:
    """Prose consisting solely of a citation marker collapses to nothing."""
    adapter = DeepAgentEventAdapter()
    adapter._emit_splits([("prose", "【cursor:1】")])
    assert adapter._prose_buf == []


@pytest.mark.asyncio
async def test_widget_citation_dedupes_by_uuid() -> None:
    """Widget citations key on the per-instance uuid, not the vendor origin."""
    adapter = DeepAgentEventAdapter()
    base = {
        "source_info": {
            "type": "widget",
            "uuid": "w-uuid-1",
            "origin": "Blackrock",
        }
    }
    adapter._absorb_citations([base, dict(base)])
    assert len(adapter._citations) == 1


@pytest.mark.asyncio
async def test_widget_citation_pages_get_separate_chips() -> None:
    """PDF widget citations include ``Page: N`` in the dedup key."""
    adapter = DeepAgentEventAdapter()
    page1 = {
        "source_info": {
            "type": "widget",
            "uuid": "doc-1",
            "metadata": {"Page": "1"},
        }
    }
    page2 = {
        "source_info": {
            "type": "widget",
            "uuid": "doc-1",
            "metadata": {"Page": "2"},
        }
    }
    adapter._absorb_citations([page1, page2, dict(page1)])
    assert len(adapter._citations) == 2


@pytest.mark.asyncio
async def test_widget_citation_falls_back_to_widget_id() -> None:
    """A widget citation with no ``uuid`` keys on ``widget_id`` instead."""
    adapter = DeepAgentEventAdapter()
    adapter._absorb_citations(
        [{"source_info": {"type": "widget", "widget_id": "slug-x"}}]
    )
    assert len(adapter._citations) == 1


@pytest.mark.asyncio
async def test_artifact_build_failure_is_swallowed() -> None:
    """An artifact payload that breaks ``_build_artifact`` logs and emits
    nothing instead of crashing the stream.
    """
    # A ``table`` artifact whose rows are not subscriptable makes the
    # records comprehension in ``_resolve_artifact_table_content`` raise.
    out = await _drive(
        DeepAgentEventAdapter(),
        [
            {
                "type": "custom",
                "data": {
                    "type": "artifact",
                    "artifact": {
                        "type": "table",
                        "name": "broken",
                        "uuid": "b1",
                        "columns": ["a"],
                        "rows": [42],
                    },
                },
            }
        ],
    )
    assert out == []


@pytest.mark.asyncio
async def test_custom_step_flushes_splitter_and_reasoning() -> None:
    """A ``step`` custom event after streamed prose flushes the splitter and
    the buffered reasoning segment before emitting the step.
    """
    raw = [
        {
            "type": "messages",
            "ns": [],
            "data": {"message": {"id": "m-1", "content": "thinking out loud "}},
        },
        {
            "type": "custom",
            "data": {"type": "step", "message": "a step"},
        },
    ]
    out = await _drive(DeepAgentEventAdapter(), raw)
    msgs = [e.data.message for e in out if isinstance(e, StatusUpdateSSE)]
    assert "thinking out loud" in " ".join(msgs)
    assert "a step" in msgs


@pytest.mark.asyncio
async def test_mcp_tool_without_inner_colon_uses_namespace_server_id() -> None:
    """An ``mcp:<fn>`` name with no inner ``:`` takes the server id from ``ns``."""
    raw = [
        {
            "type": "messages",
            "ns": ["nspace"],
            "data": {
                "message": {
                    "content": "",
                    "tool_calls": [{"name": "mcp:bare_fn", "args": {}, "id": "c-1"}],
                }
            },
        }
    ]
    out = await _drive(DeepAgentEventAdapter(), raw)
    assert isinstance(out[0], FunctionCallSSE)
    assert out[0].data.input_arguments == {
        "server_id": "nspace",
        "name": "bare_fn",
        "arguments": {},
    }


@pytest.mark.asyncio
async def test_new_message_id_rotates_splitter_and_flushes_old() -> None:
    """A new message id flushes the prior message's splitter before
    rotating to a fresh one.
    """
    raw = [
        {
            "type": "messages",
            "ns": [],
            "data": {"message": {"id": "m-1", "content": "first part "}},
        },
        {
            "type": "messages",
            "ns": [],
            "data": {"message": {"id": "m-2", "content": "second part"}},
        },
    ]
    out = await _drive(DeepAgentEventAdapter(), raw)
    delta = "".join(e.data.delta for e in out if isinstance(e, MessageChunkSSE))
    assert "first part" in delta
    assert "second part" in delta


def test_translate_messages_idless_event_makes_disposable_splitter() -> None:
    """A boundary-less (id-less) event with no splitter yet creates one."""
    adapter = DeepAgentEventAdapter()
    assert adapter._splitter is None
    adapter._translate_messages(
        {"message": {"id": "", "content": "hello", "tool_calls": []}},
        ns=("agent",),
    )
    assert adapter._splitter is not None


def test_translate_custom_inline_artifact_emits_message_artifact() -> None:
    """``_translate_custom`` with an ``artifact`` kind emits a
    ``MessageArtifactSSE`` directly (inline path).
    """
    adapter = DeepAgentEventAdapter()
    out = adapter._translate_custom(
        {
            "type": "artifact",
            "artifact": {
                "type": "text",
                "uuid": "a1",
                "name": "n",
                "content": "body",
            },
        }
    )
    assert len(out) == 1
    assert isinstance(out[0], MessageArtifactSSE)
