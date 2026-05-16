"""End-to-end checkpointer behaviour through the HTTP surface."""

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
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", json.dumps(["one", "two"]))
    with TestClient(create_app(AgentServerSettings())) as client:
        yield client


def test_run_writes_checkpoint_to_thread(client: TestClient) -> None:
    """One turn writes at least one checkpoint under the user/profile namespace."""
    resp = client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "first"}],
            "conversation_id": "conv-resume-1",
        },
    )
    assert resp.status_code == 200

    saver = client.app.state.checkpointer
    assert saver is not None

    import asyncio

    # thread_id is per-turn (includes trace_id) — multi-turn coherence
    # comes from the message list, not the checkpointer. Walk every
    # thread under this user's namespace and assert at least one was
    # written.
    items = asyncio.run(_collect_all(saver))
    threads = {it.config["configurable"]["thread_id"] for it in items}
    assert any(t.startswith("anonymous:default:") for t in threads)


def test_two_turns_each_get_their_own_thread(client: TestClient) -> None:
    """Per-turn thread_id design: two turns produce two distinct threads.

    Conversation continuity comes from the message list (which the
    client resends every turn), not from sharing a checkpointer
    thread — sharing would replay half-completed tool calls.
    """
    for content in ("turn one", "turn two"):
        client.post(
            "/v1/query",
            json={
                "messages": [{"role": "human", "content": content}],
                "conversation_id": "conv-resume-2",
            },
        )
    saver = client.app.state.checkpointer

    import asyncio

    items = asyncio.run(_collect_all(saver))
    threads = {it.config["configurable"]["thread_id"] for it in items}
    # Two turns ⇒ two distinct threads under this user/profile namespace.
    matching = [t for t in threads if t.startswith("anonymous:default:")]
    assert len(matching) >= 2


def test_thread_id_isolates_conversations(client: TestClient) -> None:
    """Distinct conversations produce distinct per-turn threads — the
    new design relies on conversation_id only for trace correlation,
    not for thread sharing.
    """
    client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "a"}],
            "conversation_id": "conv-A",
        },
    )
    client.post(
        "/v1/query",
        json={
            "messages": [{"role": "human", "content": "b"}],
            "conversation_id": "conv-B",
        },
    )
    saver = client.app.state.checkpointer

    import asyncio

    items = asyncio.run(_collect_all(saver))
    threads = {it.config["configurable"]["thread_id"] for it in items}
    matching = [t for t in threads if t.startswith("anonymous:default:")]
    # Two queries from distinct conversations ⇒ at least two threads.
    assert len(matching) >= 2


async def _collect_all(saver) -> list:
    out = []
    async for item in saver.alist(None):
        out.append(item)
    return out
