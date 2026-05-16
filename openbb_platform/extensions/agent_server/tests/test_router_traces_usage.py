"""/v1/traces/{id} and /v1/usage endpoint tests."""

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
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", '["artifacts"]')
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", json.dumps(["A simple reply."]))
    with TestClient(create_app(AgentServerSettings())) as client:
        yield client


def test_trace_endpoint_returns_full_bundle(client: TestClient) -> None:
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "hi"}],
            "conversation_id": "c-trace-1",
            "run_id": "r-trace-1",
        },
    )
    assert resp.status_code == 200
    # Server-generated per-request trace id surfaced via the
    # X-Server-Trace-ID response header. (X-Trace-ID is conversation_id.)
    server_trace_id = resp.headers["X-Server-Trace-ID"]

    bundle = client.get(f"/v1/traces/{server_trace_id}").json()
    assert bundle["trace"]["trace_id"] == server_trace_id
    assert bundle["trace"]["run_id"] == "r-trace-1"
    assert bundle["trace"]["conversation_id"] == "c-trace-1"
    assert isinstance(bundle["messages"], list)
    assert isinstance(bundle["tool_calls"], list)
    assert isinstance(bundle["usage"], list)
    assert isinstance(bundle["artifacts"], list)
    assert isinstance(bundle["citations"], list)


def test_trace_endpoint_404_for_unknown_trace(client: TestClient) -> None:
    resp = client.get("/v1/traces/does-not-exist")
    assert resp.status_code == 404


def test_trace_endpoint_404_for_other_users_trace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "bearer_static")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'shared.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BEARER", "alice")
    monkeypatch.setenv(
        "OPENBB_AGENT_AUTH_CONFIG",
        '{"token": "alice", "user_id": "alice", "scopes": ["agent:query"]}',
    )

    with TestClient(create_app(AgentServerSettings())) as alice_client:
        alice_client.headers["Authorization"] = "Bearer alice"
        alice_resp = alice_client.post(
            "/v1/query",
            json={"messages": [{"role": "human", "content": "hi"}]},
        )
        alice_trace_id = alice_resp.headers["X-Server-Trace-ID"]

        # New principal (Bob) on the same DB looking for Alice's trace.
        monkeypatch.setenv(
            "OPENBB_AGENT_AUTH_CONFIG",
            '{"token": "bob", "user_id": "bob", "scopes": ["agent:query"]}',
        )
        monkeypatch.setenv("OPENBB_AGENT_AUTH_BEARER", "bob")
        services.reset()
        with TestClient(create_app(AgentServerSettings())) as bob_client:
            bob_client.headers["Authorization"] = "Bearer bob"
            resp = bob_client.get(f"/v1/traces/{alice_trace_id}")
            assert resp.status_code == 404


def test_usage_endpoint_returns_aggregates(client: TestClient) -> None:
    # The fake model doesn't emit usage_metadata, so we expect an empty
    # by_model array — this still exercises the endpoint code path.
    client.post(
        "/v1/query",
        json={"messages": [{"role": "human", "content": "hi"}]},
    )
    resp = client.get("/v1/usage")
    assert resp.status_code == 200
    assert "by_model" in resp.json()


def test_usage_endpoint_filters_by_trace_id(client: TestClient) -> None:
    post = client.post(
        "/v1/query",
        json={"messages": [{"role": "human", "content": "hi"}]},
    )
    trace_id = post.headers["X-Server-Trace-ID"]
    resp = client.get("/v1/usage", params={"trace_id": trace_id})
    assert resp.status_code == 200
