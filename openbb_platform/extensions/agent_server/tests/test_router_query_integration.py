"""End-to-end integration test for /v1/query with the fake model provider."""

from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from openbb_agent_server.app.app import create_app
from openbb_agent_server.app.settings import AgentServerSettings
from openbb_agent_server.observability.logging import TRACE
from openbb_agent_server.runtime import services


@pytest.fixture
def client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> Iterator[TestClient]:
    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")
    # The fake model returns this canned reply regardless of input.
    monkeypatch.setenv(
        "OPENBB_AGENT_FAKE_RESPONSES",
        json.dumps(["Hello from the fake model."]),
    )
    settings = AgentServerSettings()
    with TestClient(create_app(settings)) as client:
        yield client


def _parse_sse(raw: str) -> list[tuple[str, dict]]:
    # SSE frames are separated by a blank line. The W3C spec allows
    # CRLF, LF, or CR — sse-starlette uses CRLF, so we normalise first.
    normalised = raw.replace("\r\n", "\n").replace("\r", "\n")
    out: list[tuple[str, dict]] = []
    for block in normalised.strip().split("\n\n"):
        event_name = ""
        payload = ""
        for line in block.splitlines():
            if line.startswith("event:"):
                event_name = line[len("event:") :].strip()
            elif line.startswith("data:"):
                payload = line[len("data:") :].strip()
        if event_name and payload:
            out.append((event_name, json.loads(payload)))
    return out


def test_query_runs_real_agent_loop_with_fake_model(client: TestClient) -> None:
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "Say hi."}],
            "conversation_id": "conv-fixture-1",
            "run_id": "run-fixture-1",
        },
        # X-Trace-ID is Workspace's conversation id; body wins.
        headers={"X-Trace-ID": "ignored-because-body-sets-conv"},
    )
    assert resp.status_code == 200
    assert resp.headers["X-Trace-ID"] == "conv-fixture-1"
    events = _parse_sse(resp.text)

    # Somewhere in the stream we should see the fake model's content.
    text_chunks = [b["delta"] for n, b in events if n == "copilotMessageChunk"]
    full = "".join(text_chunks)
    assert "Hello from the fake model" in full or any(
        "Hello from the fake model" in (b.get("message") or "") for n, b in events
    )


def test_xtraceid_header_is_treated_as_conversation_id(client: TestClient) -> None:
    """``X-Trace-ID`` from Workspace IS the conversation id."""
    body = {"messages": [{"role": "human", "content": "hi"}]}
    headers = {"X-Trace-ID": "conv-from-header"}
    r1 = client.post("/v1/query", json=body, headers=headers)
    r2 = client.post("/v1/query", json=body, headers=headers)
    assert r1.status_code == 200
    assert r2.status_code == 200, r2.text
    # Both turns echo the same conversation id — that's the contract.
    assert r1.headers["X-Trace-ID"] == "conv-from-header"
    assert r2.headers["X-Trace-ID"] == "conv-from-header"


def test_body_conversation_id_wins_over_xtraceid_header(client: TestClient) -> None:
    """When the body sets ``conversation_id``, the header is ignored."""
    body = {
        "messages": [{"role": "human", "content": "hi"}],
        "conversation_id": "from-body",
    }
    resp = client.post("/v1/query", json=body, headers={"X-Trace-ID": "from-header"})
    assert resp.status_code == 200
    assert resp.headers["X-Trace-ID"] == "from-body"


def test_query_never_broadcasts_middleware_lifecycle_steps(
    client: TestClient,
) -> None:
    """LangGraph internal middleware steps must never reach the UI."""
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "hi"}],
            "conversation_id": "no-mw-noise",
        },
    )
    assert resp.status_code == 200
    events = _parse_sse(resp.text)
    bad_substrings = (
        "Middleware.before_",
        "Middleware.after_",
        "_Middleware",
    )
    for name, body in events:
        if name != "copilotStatusUpdate":
            continue
        msg = body.get("message") or ""
        for needle in bad_substrings:
            assert needle not in msg, (
                f"middleware-lifecycle status leaked to UI: {msg!r}"
            )


def test_query_does_not_leak_trace_started_status_update(
    client: TestClient,
) -> None:
    """The trace_id is server-internal — never broadcast as a UI status step."""
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "Say hi."}],
            "conversation_id": "conv-no-trace-step",
        },
        headers={"X-Trace-ID": "conv-no-trace-step"},
    )
    assert resp.status_code == 200
    # ``X-Trace-ID`` mirrors the resolved conversation id back.
    assert resp.headers["X-Trace-ID"] == "conv-no-trace-step"
    events = _parse_sse(resp.text)
    for name, body in events:
        if name == "copilotStatusUpdate":
            assert body.get("message") != "trace started", (
                "trace-started status updates are debug-only; never broadcast"
            )


def test_query_persists_human_and_ai_messages(client: TestClient) -> None:
    client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "Persist me."}],
            "conversation_id": "conv-persist-1",
        },
    )
    resp = client.get("/v1/conversations/conv-persist-1/messages")
    assert resp.status_code == 200
    msgs = resp.json()["messages"]
    roles = [m["role"] for m in msgs]
    contents = [m["content"] for m in msgs]
    assert "human" in roles
    assert any(c == "Persist me." for c in contents)
    # AI turn captured as well (assembled chunks).
    assert "ai" in roles


def test_query_generates_conversation_id_when_neither_body_nor_header_supplies_one(
    client: TestClient,
) -> None:
    resp = client.post(
        "/v1/query",
        json={"messages": [{"role": "human", "content": "hi"}]},
    )
    assert resp.status_code == 200
    # ``X-Trace-ID`` mirrors the conversation id; server fell back to UUID.
    conversation_id = resp.headers["X-Trace-ID"]
    assert conversation_id  # non-empty UUID


def test_query_returns_distinct_generated_conversation_ids_when_unset(
    client: TestClient,
) -> None:
    """No conversation_id supplied → fresh UUID per call."""
    r1 = client.post(
        "/v1/query",
        json={"messages": [{"role": "human", "content": "a"}]},
    )
    r2 = client.post(
        "/v1/query",
        json={"messages": [{"role": "human", "content": "b"}]},
    )
    assert r1.headers["X-Trace-ID"] != r2.headers["X-Trace-ID"]


def test_delete_me_purges_user_history(client: TestClient) -> None:
    client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "x"}],
            "conversation_id": "c-purge-1",
        },
    )
    # Confirm conversation exists.
    assert len(client.get("/v1/conversations").json()["conversations"]) == 1
    resp = client.delete("/v1/me")
    assert resp.status_code == 204
    assert client.get("/v1/conversations").json()["conversations"] == []


def test_query_builder_astream_exception_propagates(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """An exception inside ``agent.astream`` surfaces as an ERROR SSE frame."""
    import deepagents

    class _BrokenAgent:
        async def astream(self, *_a: object, **_kw: object):
            raise RuntimeError("astream kaboom")
            yield  # pragma: no cover

    def fake_create_deep_agent(**_kw: object) -> _BrokenAgent:
        return _BrokenAgent()

    monkeypatch.setattr(deepagents, "create_deep_agent", fake_create_deep_agent)
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "hi"}],
            "conversation_id": "astream-fail-1",
        },
    )
    assert resp.status_code == 200
    assert "astream kaboom" in resp.text


def test_query_with_prior_tool_message_narrows_tools_in_builder(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    """A body that already carries a tool message exercises ``has_ingested`` narrowing."""
    import openbb_agent_server.runtime.builder as builder_mod

    captured: dict[str, list[str]] = {"tool_names": []}
    original_resolve = builder_mod._resolve_tools

    async def watching_resolve(
        ctx: object, profile: object
    ) -> tuple[list[object], frozenset[str]]:
        tools, clients = await original_resolve(ctx, profile)
        # Snapshot the tool name list BEFORE narrowing so we can compare.
        captured["tool_names"] = [getattr(t, "name", "") for t in tools]
        return tools, clients

    monkeypatch.setattr(builder_mod, "_resolve_tools", watching_resolve)
    body = {
        "messages": [
            {"role": "human", "content": "Get widget data."},
            {
                "role": "ai",
                "content": (
                    '{"function": "get_widget_data", "input_arguments": '
                    '{"data_sources": [{"widget_uuid": "w-1", "id": "px", '
                    '"input_args": {}}]}}'
                ),
            },
            {
                "role": "tool",
                "tool_call_id": "c1",
                "data": [{"items": [{"px": 100}]}],
            },
            {"role": "human", "content": "now summarise"},
        ],
        "conversation_id": "tool-narrow-1",
    }
    resp = client.post("/v1/query", json=body)
    assert resp.status_code == 200


def test_query_trace_logging_dumps_body(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """Every ``/v1/query`` writes the raw request body to a temp file.

    The path is logged at INFO; the file contains the verbatim wire
    bytes so operators can inspect what Workspace actually sent.
    """
    import logging

    from openbb_agent_server.observability.logging import TRACE

    router_logger = logging.getLogger("openbb_agent_server.router")
    router_logger.setLevel(TRACE)
    caplog.set_level(TRACE, logger="openbb_agent_server.router")
    long_content = "x" * 800
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": long_content}],
            "conversation_id": "trace-dump-1",
            "timezone": "America/New_York",
        },
    )
    assert resp.status_code == 200
    raw_lines = [r.getMessage() for r in caplog.records]
    dump_line = next(
        (line for line in raw_lines if "request body dumped" in line),
        None,
    )
    assert dump_line is not None
    # Path is included so a human can `cat` it.
    assert "path=" in dump_line
    # The non-``messages`` body keys still flow through the TRACE
    # ``body.X=…`` branch when TRACE is enabled.
    assert any("body." in line for line in raw_lines)


def test_query_trace_logging_truncates_large_body_blob(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """A non-``messages`` body key larger than 4 KB gets the ``...<N chars>`` suffix."""
    import logging

    from openbb_agent_server.observability.logging import TRACE

    router_logger = logging.getLogger("openbb_agent_server.router")
    router_logger.setLevel(TRACE)
    caplog.set_level(TRACE, logger="openbb_agent_server.router")
    # Build api_keys with enough bytes to push the JSON blob past 4 KB.
    big_api_keys = {f"K{i:03d}": "x" * 80 for i in range(80)}
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "hi"}],
            "conversation_id": "trace-blob-1",
            "api_keys": big_api_keys,
        },
    )
    assert resp.status_code == 200
    # The PII filter redacts ``api_keys`` to ``<redacted-key>`` in the
    # rendered line, but the trailing ``...<N chars>`` truncation suffix
    # still appears because the source blob exceeded 4 KB.
    assert any(
        "body." in r.getMessage() and "chars>" in r.getMessage() for r in caplog.records
    )


def test_query_widget_ingest_skips_data_sources_with_blank_uuid(
    client: TestClient,
) -> None:
    """A ``get_widget_data`` data_source with no widget_uuid is skipped silently."""
    resp = client.post(
        "/v1/query",
        json={
            "messages": [
                {"role": "human", "content": "show me"},
                {
                    "role": "ai",
                    "content": (
                        '{"function": "get_widget_data", "input_arguments": '
                        '{"data_sources": [{"widget_uuid": "", "id": "px", '
                        '"input_args": {}}]}}'
                    ),
                },
                {
                    "role": "tool",
                    "tool_call_id": "c1",
                    "data": [{"items": [{"px": 100}]}],
                },
                {"role": "human", "content": "ok"},
            ],
            "conversation_id": "blank-widget-uuid-1",
        },
    )
    assert resp.status_code == 200


def test_query_with_long_message_content_truncates_trace_dump(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """The per-message dump truncates content past 400 chars."""
    import logging

    from openbb_agent_server.observability.logging import TRACE

    router_logger = logging.getLogger("openbb_agent_server.router")
    router_logger.setLevel(TRACE)
    caplog.set_level(TRACE, logger="openbb_agent_server.router")
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "a" * 800}],
            "conversation_id": "trace-trunc-1",
        },
    )
    assert resp.status_code == 200
    # Truncated marker appears in the message-dump line.
    assert any(
        "chars>" in r.getMessage() and "msg[" in r.getMessage() for r in caplog.records
    )


def test_query_widget_ingest_swallows_storage_exception(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failing ``WidgetDataStore.record`` is logged but doesn't abort the run."""
    import logging

    from openbb_agent_server.runtime import services
    from openbb_agent_server.runtime.widget_store import WidgetDataStore

    async def boom(self: WidgetDataStore, **_kw: object) -> int:
        raise RuntimeError("widget store down")

    monkeypatch.setattr(WidgetDataStore, "record", boom)
    # Drive a query whose body contains a recognisable ``get_widget_data``
    # ai/tool sequence so the router enters the ingestion path.
    body = {
        "messages": [
            {"role": "human", "content": "What's the price?"},
            {
                "role": "ai",
                "content": (
                    '{"function": "get_widget_data", "input_arguments": '
                    '{"data_sources": [{"widget_uuid": "w-1", "id": "px", '
                    '"input_args": {}}]}}'
                ),
            },
            {
                "role": "tool",
                "tool_call_id": "c1",
                "data": [{"items": [{"px": 100}]}],
            },
            {"role": "human", "content": "now what?"},
        ],
        # ``w-1`` must be on the dashboard for the router to attempt
        # ingestion — off-dashboard widget results are skipped before
        # ``record`` is ever called. No inline ``data`` so only the
        # tool-message ingestion path fires, not the inline path.
        "widgets": {
            "primary": [{"uuid": "w-1", "widget_id": "px", "origin": "market"}],
            "secondary": [],
            "extra": [],
        },
        "conversation_id": "ingest-fail-1",
    }
    # Ensure widget_store is bound (it is, because the fixture brings it up).
    assert services.get_widget_store() is not None
    with caplog.at_level(logging.WARNING):
        resp = client.post("/v1/query", json=body)
    assert resp.status_code == 200
    assert any("ingestion failed" in r.message for r in caplog.records)


def test_query_ingest_request_context_swallows_failure(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failure inside ``ingest_request_context`` is logged but the run continues."""
    import logging

    import openbb_agent_server.app.router as router_mod

    async def boom(**_kw: object) -> int:
        raise RuntimeError("ingest detonated")

    monkeypatch.setattr(router_mod, "ingest_request_context", boom)
    with caplog.at_level(logging.WARNING):
        resp = client.post(
            "/v1/query",
            json={
                "messages": [{"role": "human", "content": "hi"}],
                "conversation_id": "ingest-fail-2",
            },
        )
    assert resp.status_code == 200
    assert any("context ingestion errored" in r.message for r in caplog.records)


def test_query_propagates_agent_run_exception_as_error_status(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """An exception inside ``run_agent`` becomes a final ``StatusUpdateSSE`` ERROR."""
    import openbb_agent_server.app.router as router_mod

    async def broken_run_agent(**_kw: object):
        raise RuntimeError("agent exploded")
        yield  # pragma: no cover - never reached

    monkeypatch.setattr(router_mod, "run_agent", broken_run_agent)
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "hi"}],
            "conversation_id": "run-fail-1",
        },
    )
    # The SSE stream still completes (200) and carries an ERROR status.
    assert resp.status_code == 200
    assert "agent exploded" in resp.text


def test_query_handles_graph_bubble_up_paused_state(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """``GraphBubbleUp`` ends the stream cleanly and marks the trace ``paused``."""
    from langgraph.errors import GraphBubbleUp

    import openbb_agent_server.app.router as router_mod

    async def paused_run_agent(**_kw: object):
        raise GraphBubbleUp("interrupt")
        yield  # pragma: no cover - never reached

    monkeypatch.setattr(router_mod, "run_agent", paused_run_agent)
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "hi"}],
            "conversation_id": "paused-1",
        },
    )
    assert resp.status_code == 200
    # Trace status persisted as ``paused``.
    traces = client.get("/v1/traces/" + resp.headers["X-Server-Trace-ID"]).json()
    assert traces["trace"]["status"] == "paused"


def test_query_logs_end_trace_failure_after_run_error(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If ``end_trace`` fails after the run errored, the failure is logged."""
    import logging

    import openbb_agent_server.app.router as router_mod
    from openbb_agent_server.runtime import services

    async def broken_run_agent(**_kw: object):
        raise RuntimeError("agent kaboom")
        yield  # pragma: no cover - never reached

    monkeypatch.setattr(router_mod, "run_agent", broken_run_agent)

    # Wrap end_trace so the ``status="error"`` cleanup raises.
    history = services.get_history()
    original = history.end_trace

    async def patched(*args: object, **kwargs: object) -> None:
        if kwargs.get("status") == "error":
            raise RuntimeError("end_trace failed after error")
        await original(*args, **kwargs)

    monkeypatch.setattr(history, "end_trace", patched)
    with caplog.at_level(logging.WARNING):
        resp = client.post(
            "/v1/query",
            json={
                "messages": [{"role": "human", "content": "hi"}],
                "conversation_id": "endtrace-fail-error",
            },
        )
    assert resp.status_code == 200
    assert any("end_trace failed after error path" in r.message for r in caplog.records)


def test_query_logs_end_trace_failure_after_graph_bubble_up(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If ``end_trace(status='paused')`` fails after a ``GraphBubbleUp``, log it."""
    import logging

    from langgraph.errors import GraphBubbleUp

    import openbb_agent_server.app.router as router_mod
    from openbb_agent_server.runtime import services

    async def paused_run_agent(**_kw: object):
        raise GraphBubbleUp("interrupt")
        yield  # pragma: no cover - never reached

    monkeypatch.setattr(router_mod, "run_agent", paused_run_agent)

    history = services.get_history()
    original = history.end_trace

    async def patched(*args: object, **kwargs: object) -> None:
        if kwargs.get("status") == "paused":
            raise RuntimeError("end_trace failed mid-pause")
        await original(*args, **kwargs)

    monkeypatch.setattr(history, "end_trace", patched)
    with caplog.at_level(logging.WARNING):
        resp = client.post(
            "/v1/query",
            json={
                "messages": [{"role": "human", "content": "hi"}],
                "conversation_id": "endtrace-fail-paused",
            },
        )
    assert resp.status_code == 200
    assert any("end_trace failed after interrupt" in r.message for r in caplog.records)


def test_agents_json_drops_profile_with_reserved_custom_feature_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``web-search`` (or any reserved alias) as a CUSTOM feature → profile dropped."""
    import logging

    from openbb_agent_server.app.app import create_app
    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.runtime import services

    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')
    # Make the default profile illegal: a custom feature using a reserved
    # web-search alias (``web-search`` itself is reserved as a custom name).
    monkeypatch.setenv(
        "OPENBB_AGENT_FEATURES",
        '{"web-search": {"label": "Web", "description": "use the web"}}',
    )
    settings = AgentServerSettings()
    with caplog.at_level(logging.WARNING), TestClient(create_app(settings)) as c:
        resp = c.get("/agents.json")
    assert resp.status_code == 200
    # Default profile was dropped because the reserved custom-feature name
    # raised ``ValueError`` inside ``_coerce_custom_feature``.
    assert "default" not in resp.json()
    assert any("invalid config" in r.message for r in caplog.records)


def test_query_logs_uploaded_files_and_file_like_widgets(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """An uploaded file + a file-like widget exercise the per-file log loops."""

    body = {
        "messages": [{"role": "human", "content": "hi"}],
        "conversation_id": "log-uploads-1",
        "uploaded_files": [
            {
                "name": "doc.pdf",
                "mime": "application/pdf",
                "url": "https://x/doc.pdf",
                "extra_meta": "v",
            }
        ],
        "widgets": {
            "primary": [
                {
                    "uuid": "fw-1",
                    "widget_id": "file-undefined",
                    "name": "Uploaded File",
                    "origin": "hub",
                    "data": [{"row": 1}],
                    "blob": "z" * 250,
                }
            ],
            "secondary": [],
            "extra": [],
        },
    }
    with caplog.at_level(TRACE, logger="openbb_agent_server.router"):
        resp = client.post("/v1/query", json=body)
    assert resp.status_code == 200
    msgs = [r.getMessage() for r in caplog.records]
    assert any("uploaded_files[0]" in m for m in msgs)
    assert any("FILE-LIKE" in m for m in msgs)


def test_query_ingests_on_dashboard_widget_tool_result(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """A tool-message widget result for an on-dashboard widget is recorded."""

    body = {
        "messages": [
            {"role": "human", "content": "price?"},
            {
                "role": "ai",
                "content": (
                    '{"function": "get_widget_data", "input_arguments": '
                    '{"data_sources": [{"widget_uuid": "w-99", "id": "px", '
                    '"input_args": {}}]}}'
                ),
            },
            {
                "role": "tool",
                "tool_call_id": "c1",
                "data": [{"items": [{"px": 100}, {"px": 101}]}],
            },
            {"role": "human", "content": "thanks"},
        ],
        "widgets": {
            "primary": [{"uuid": "w-99", "widget_id": "px", "origin": "market"}],
            "secondary": [],
            "extra": [],
        },
        "conversation_id": "ingest-ok-1",
    }
    with caplog.at_level(TRACE, logger="openbb_agent_server.router"):
        resp = client.post("/v1/query", json=body)
    assert resp.status_code == 200
    msgs = [r.getMessage() for r in caplog.records]
    assert any("tool-message widget result" in m for m in msgs)


def test_query_skips_error_payload_tool_result(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """A ``[{error_type, ...}]`` tool result is flagged ``is_error`` and skipped."""

    body = {
        "messages": [
            {"role": "human", "content": "price?"},
            {
                "role": "ai",
                "content": (
                    '{"function": "get_widget_data", "input_arguments": '
                    '{"data_sources": [{"widget_uuid": "w-err", "id": "px", '
                    '"input_args": {}}]}}'
                ),
            },
            {
                "role": "tool",
                "tool_call_id": "c1",
                "data": [{"error_type": "not_found", "message": "missing"}],
            },
            {"role": "human", "content": "ok"},
        ],
        "widgets": {
            "primary": [{"uuid": "w-err", "widget_id": "px", "origin": "market"}],
            "secondary": [],
            "extra": [],
        },
        "conversation_id": "ingest-err-1",
    }
    with caplog.at_level(TRACE, logger="openbb_agent_server.router"):
        resp = client.post("/v1/query", json=body)
    assert resp.status_code == 200
    msgs = [r.getMessage() for r in caplog.records]
    assert any("is_error=True" in m for m in msgs)


def test_query_ingests_inline_widget_data(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """A primary widget carrying inline ``data`` rows is ingested directly.

    No prior tool message — so the pre-empt ``get_widget_data`` path is
    skipped (the inline-data guard fires) and the inline-ingest loop runs.
    """

    body = {
        "messages": [{"role": "human", "content": "summarise this"}],
        "widgets": {
            "primary": [
                {
                    "uuid": "inline-1",
                    "widget_id": "csv-upload",
                    "name": "Spreadsheet",
                    "origin": "hub",
                    "data": [{"col_a": 1, "col_b": 2}],
                }
            ],
            "secondary": [],
            "extra": [],
        },
        "conversation_id": "inline-ingest-1",
    }
    with caplog.at_level(TRACE, logger="openbb_agent_server.router"):
        resp = client.post("/v1/query", json=body)
    assert resp.status_code == 200
    msgs = [r.getMessage() for r in caplog.records]
    assert any("inline widget(s)" in m for m in msgs)


def test_query_closes_stream_on_function_call_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A ``FunctionCallSSE`` from the agent terminates the SSE immediately."""

    import openbb_agent_server.app.router as router_mod
    from openbb_agent_server.protocol.schemas import (
        FunctionCallSSE,
        FunctionCallSSEData,
    )

    async def fcall_run_agent(**_kw: object):
        yield FunctionCallSSE(
            data=FunctionCallSSEData(
                function="get_widget_data",
                input_arguments={"data_sources": []},
            )
        )

    monkeypatch.setattr(router_mod, "run_agent", fcall_run_agent)
    with caplog.at_level(TRACE, logger="openbb_agent_server.router"):
        resp = client.post(
            "/v1/query",
            json={
                "messages": [{"role": "human", "content": "hi"}],
                "conversation_id": "fcall-dispatch-1",
            },
        )
    assert resp.status_code == 200
    assert "copilotFunctionCall" in resp.text
    msgs = [r.getMessage() for r in caplog.records]
    assert any("closing after function call dispatch" in m for m in msgs)


def test_query_raw_body_dump_failure_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failure while dumping the raw request body is logged, not raised."""

    import openbb_agent_server.app.router as router_mod

    real_write_bytes = router_mod._PathlibPath.write_bytes

    def boom(self: object, data: bytes) -> int:
        raise OSError("disk full")

    monkeypatch.setattr(router_mod._PathlibPath, "write_bytes", boom)
    try:
        with caplog.at_level(TRACE, logger="openbb_agent_server.router"):
            resp = client.post(
                "/v1/query",
                json={
                    "messages": [{"role": "human", "content": "hi"}],
                    "conversation_id": "rawdump-fail-1",
                },
            )
    finally:
        monkeypatch.setattr(router_mod._PathlibPath, "write_bytes", real_write_bytes)
    assert resp.status_code == 200
    assert any("RAW BODY dump failed" in r.message for r in caplog.records)


def test_cancel_sets_event_for_callers_in_flight_run(client: TestClient) -> None:
    """A cancel request flips the cancel-event for the caller's active runs."""
    import asyncio as _asyncio

    import openbb_agent_server.app.router as router_mod

    ev = _asyncio.Event()
    # ``none`` auth resolves to the anonymous principal.
    router_mod._cancellations[("anonymous", "run-xyz")] = ev
    try:
        resp = client.post("/v1/conversations/conv-1/cancel")
        assert resp.status_code == 202
        assert resp.json()["cancelled_runs"] == ["run-xyz"]
        assert ev.is_set()
    finally:
        router_mod._cancellations.pop(("anonymous", "run-xyz"), None)


def test_query_logs_file_like_widget_with_dict_and_scalar_data(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    """File-like widgets whose ``data`` is a dict / scalar hit both preview arms."""

    body = {
        "messages": [{"role": "human", "content": "hi"}],
        "conversation_id": "file-like-shapes-1",
        "widgets": {
            "primary": [
                {
                    "uuid": "fw-dict",
                    "widget_id": "document-dict",
                    "data": {"k1": "v1", "k2": "v2"},
                },
                {
                    "uuid": "fw-scalar",
                    "widget_id": "filing-scalar",
                    "data": "raw string payload",
                },
            ],
            "secondary": [],
            "extra": [],
        },
    }
    with caplog.at_level(TRACE, logger="openbb_agent_server.router"):
        resp = client.post("/v1/query", json=body)
    assert resp.status_code == 200
    msgs = [r.getMessage() for r in caplog.records]
    assert sum("FILE-LIKE" in m for m in msgs) >= 2


def test_query_inline_ingest_skips_widget_with_no_uuid(
    client: TestClient,
) -> None:
    """An inline-data widget with neither uuid nor widget_id is skipped."""
    body = {
        "messages": [{"role": "human", "content": "summarise"}],
        "widgets": {
            "primary": [{"uuid": "", "widget_id": "", "data": [{"a": 1}]}],
            "secondary": [],
            "extra": [],
        },
        "conversation_id": "inline-no-uuid-1",
    }
    resp = client.post("/v1/query", json=body)
    assert resp.status_code == 200
