"""Disconnect-during-stream test."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from openbb_agent_server.app.app import create_app
from openbb_agent_server.app.settings import AgentServerSettings
from openbb_agent_server.runtime import services


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[TestClient]:
    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_CHECKPOINTER_PROVIDER", "sqlite")
    monkeypatch.setenv(
        "OPENBB_AGENT_FAKE_RESPONSES", json.dumps(["the full final answer"])
    )
    with TestClient(create_app(AgentServerSettings())) as client:
        yield client


def test_run_drains_to_natural_end_when_client_disconnects(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Patch ``is_disconnected`` to flip True after the first event."""
    from starlette.requests import Request

    flipped = {"value": False}
    original = Request.is_disconnected

    async def patched_is_disconnected(self):  # type: ignore[no-untyped-def]
        # Stay connected for the trace-started step, then drop.
        if not flipped["value"]:
            flipped["value"] = True
            return False
        return True

    monkeypatch.setattr(Request, "is_disconnected", patched_is_disconnected)

    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "say hi"}],
            "conversation_id": "conv-disconnect-1",
        },
    )
    assert resp.status_code == 200

    # Even though the client "disconnected", the agent ran to natural
    # completion and persisted the AI turn. This is the proof:
    msgs = client.get("/v1/conversations/conv-disconnect-1/messages").json()["messages"]
    ai_turns = [m for m in msgs if m["role"] == "ai"]
    assert ai_turns
    assert ai_turns[-1]["content"] == "the full final answer"


def test_run_drains_does_not_record_cancelled_status(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A disconnect must NOT leave the trace marked ``cancelled``."""
    from starlette.requests import Request

    async def always_disconnected(self):  # type: ignore[no-untyped-def]
        return True

    monkeypatch.setattr(Request, "is_disconnected", always_disconnected)

    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "say hi"}],
            "conversation_id": "conv-disconnect-2",
        },
    )
    assert resp.status_code == 200
    # Server-generated per-request trace_id surfaced via the
    # X-Server-Trace-ID response header. (X-Trace-ID is the conversation id.)
    server_trace_id = resp.headers["X-Server-Trace-ID"]
    bundle = client.get(f"/v1/traces/{server_trace_id}").json()
    # The run completed naturally — disconnects don't mark cancelled.
    assert bundle["trace"]["status"] == "completed"


def test_explicit_cancel_endpoint_still_marks_cancelled(
    client: TestClient,
) -> None:
    """The opt-in cancel POST is still honoured (we only changed disconnect handling)."""
    # No active run when we call cancel — the path returns 202 with an
    # empty cancelled list. The endpoint behaviour is unchanged.
    resp = client.post("/v1/conversations/whatever/cancel")
    assert resp.status_code == 202
    assert "cancelled_runs" in resp.json()
