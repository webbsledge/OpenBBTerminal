"""Builder stream-event normalisation tests."""

from __future__ import annotations

from langchain_core.messages import AIMessageChunk

from openbb_agent_server.runtime.builder import _normalise_stream_event


def test_normalise_messages_event() -> None:
    """The normaliser projects an AIMessageChunk into a stable dict."""
    msg = AIMessageChunk(content="hi", tool_calls=[])
    out = _normalise_stream_event(((), "messages", (msg, {"meta": "x"})))
    assert out is not None
    assert out["type"] == "messages"
    assert out["ns"] == []
    message = out["data"]["message"]
    assert message["content"] == "hi"
    assert message["tool_calls"] == []
    assert message["tool_call_chunks"] == []
    assert message["additional_kwargs"] == {}
    assert "id" in message


def test_normalise_messages_event_with_tool_calls() -> None:
    msg = AIMessageChunk(
        content="",
        tool_calls=[{"name": "x", "args": {}, "id": "c1"}],
    )
    out = _normalise_stream_event(((), "messages", (msg, {})))
    assert out is not None
    assert out["data"]["message"]["tool_calls"][0]["name"] == "x"


def test_normalise_messages_event_missing_payload_drops() -> None:
    out = _normalise_stream_event(((), "messages", ()))
    assert out is None


def test_normalise_updates_event() -> None:
    out = _normalise_stream_event(((), "updates", {"agent": {"messages": []}}))
    assert out == {
        "type": "updates",
        "ns": [],
        "data": {"agent": {"messages": []}},
    }


def test_normalise_updates_with_none_data() -> None:
    out = _normalise_stream_event(((), "updates", None))
    assert out == {"type": "updates", "ns": [], "data": {}}


def test_normalise_custom_dict_passthrough() -> None:
    payload = {"type": "chunk", "content": "x"}
    out = _normalise_stream_event(((), "custom", payload))
    assert out == {"type": "custom", "ns": [], "data": payload}


def test_normalise_custom_non_dict_wraps_as_message() -> None:
    out = _normalise_stream_event(((), "custom", "raw text"))
    assert out == {"type": "custom", "ns": [], "data": {"message": "raw text"}}


def test_normalise_unknown_mode_returns_none() -> None:
    assert _normalise_stream_event(((), "unknown_mode", {})) is None


def test_normalise_non_tuple_returns_none() -> None:
    assert _normalise_stream_event("not a tuple") is None


def test_normalise_wrong_arity_tuple_returns_none() -> None:
    assert _normalise_stream_event(("a", "b")) is None


def test_normalise_subagent_namespace_propagates() -> None:
    msg = AIMessageChunk(content="from-subagent")
    out = _normalise_stream_event((("tools:abc",), "messages", (msg, {})))
    assert out is not None
    assert out["ns"] == ["tools:abc"]


def test_normalise_messages_event_with_none_content_substitutes_empty_string() -> None:
    """An AIMessageChunk whose content is None normalises to an empty string."""
    msg = AIMessageChunk(content="")
    object.__setattr__(msg, "content", None)
    out = _normalise_stream_event(((), "messages", (msg, {})))
    assert out is not None
    assert out["data"]["message"]["content"] == ""
