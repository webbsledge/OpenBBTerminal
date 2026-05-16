"""SQLite HistoryStore round-trip + isolation tests."""

from __future__ import annotations

from typing import Any

import pytest

from openbb_agent_server.persistence.sqlite_store import SqliteHistoryStore
from openbb_agent_server.persistence.store import UsageRecord
from openbb_agent_server.runtime.principal import UserPrincipal


@pytest.mark.asyncio
async def test_upsert_user_then_append_message_round_trip(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    seq = await history.append_message(
        principal=alice,
        conversation_id="c1",
        role="human",
        content="hello",
        trace_id="t1",
    )
    assert seq == 0
    seq2 = await history.append_message(
        principal=alice,
        conversation_id="c1",
        role="ai",
        content="hi back",
        trace_id="t1",
    )
    assert seq2 == 1

    msgs = await history.get_messages(principal=alice, conversation_id="c1")
    assert [m.role for m in msgs] == ["human", "ai"]
    assert [m.content for m in msgs] == ["hello", "hi back"]


@pytest.mark.asyncio
async def test_list_conversations_returns_only_caller_threads(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
    bob: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.upsert_user(bob)
    await history.append_message(
        principal=alice,
        conversation_id="c-alice",
        role="human",
        content="x",
        trace_id=None,
    )
    await history.append_message(
        principal=bob, conversation_id="c-bob", role="human", content="y", trace_id=None
    )

    alices = await history.list_conversations(principal=alice)
    bobs = await history.list_conversations(principal=bob)
    assert {c["conversation_id"] for c in alices} == {"c-alice"}
    assert {c["conversation_id"] for c in bobs} == {"c-bob"}


@pytest.mark.asyncio
async def test_cross_user_message_append_is_rejected(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
    bob: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.upsert_user(bob)
    await history.append_message(
        principal=alice, conversation_id="c1", role="human", content="hi", trace_id=None
    )
    with pytest.raises(PermissionError):
        await history.append_message(
            principal=bob,
            conversation_id="c1",  # belongs to alice
            role="human",
            content="sneaky",
            trace_id=None,
        )


@pytest.mark.asyncio
async def test_get_messages_for_other_users_thread_returns_empty(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
    bob: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.upsert_user(bob)
    await history.append_message(
        principal=alice,
        conversation_id="c1",
        role="human",
        content="secret",
        trace_id=None,
    )
    bobs_view = await history.get_messages(principal=bob, conversation_id="c1")
    assert bobs_view == []


@pytest.mark.asyncio
async def test_record_usage_round_trip(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.begin_trace(
        principal=alice, trace_id="t1", conversation_id=None, run_id=None
    )
    usage = UsageRecord(
        trace_id="t1",
        user_id=alice.user_id,
        model="claude-opus-4-7",
        input_tokens=100,
        output_tokens=200,
        cache_read=10,
        cache_creation=5,
        cost_usd=0.0042,
    )
    await history.record_usage(principal=alice, trace_id="t1", usage=usage)


@pytest.mark.asyncio
async def test_record_usage_rejects_user_id_mismatch(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
    bob: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.upsert_user(bob)
    await history.begin_trace(
        principal=alice, trace_id="t1", conversation_id=None, run_id=None
    )
    bad = UsageRecord(
        trace_id="t1",
        user_id=alice.user_id,  # forging
        model="m",
        input_tokens=1,
        output_tokens=1,
        cache_read=0,
        cache_creation=0,
        cost_usd=0.0,
    )
    with pytest.raises(PermissionError):
        await history.record_usage(principal=bob, trace_id="t1", usage=bad)


@pytest.mark.asyncio
async def test_delete_user_cascade_wipes_data(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
    bob: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.upsert_user(bob)
    await history.append_message(
        principal=alice, conversation_id="c1", role="human", content="x", trace_id=None
    )
    await history.append_message(
        principal=bob, conversation_id="c2", role="human", content="y", trace_id=None
    )
    await history.delete_user(alice)
    # Alice's rows are gone…
    assert await history.list_conversations(principal=alice) == []
    # …but Bob's are intact.
    bobs = await history.list_conversations(principal=bob)
    assert {c["conversation_id"] for c in bobs} == {"c2"}


@pytest.mark.asyncio
async def test_record_tool_call_round_trip(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.begin_trace(
        principal=alice, trace_id="t1", conversation_id=None, run_id=None
    )
    await history.record_tool_call(
        principal=alice,
        trace_id="t1",
        tool_name="search",
        args={"q": "AAPL"},
        result={"hits": 3},
        error=None,
        latency_ms=42,
        side="server",
        state="complete",
    )


@pytest.mark.asyncio
async def test_end_trace_updates_status(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.begin_trace(
        principal=alice, trace_id="t1", conversation_id=None, run_id=None
    )
    await history.end_trace(principal=alice, trace_id="t1", status="completed")
    # Re-end should be idempotent.
    await history.end_trace(principal=alice, trace_id="t1", status="completed")


@pytest.mark.asyncio
async def test_end_trace_for_other_users_trace_is_noop(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
    bob: UserPrincipal,
) -> None:
    await history.upsert_user(alice)
    await history.upsert_user(bob)
    await history.begin_trace(
        principal=alice, trace_id="t1", conversation_id=None, run_id=None
    )
    # Bob trying to mark Alice's trace ends silently — nothing changes.
    await history.end_trace(principal=bob, trace_id="t1", status="error")


@pytest.mark.asyncio
async def test_begin_trace_resumes_existing_trace_with_new_run_id(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
) -> None:
    """Re-begin reuses the trace row and resets ``ended_at`` / ``status``."""
    await history.upsert_user(alice)
    await history.begin_trace(
        principal=alice, trace_id="t1", conversation_id="c1", run_id="r1"
    )
    await history.end_trace(principal=alice, trace_id="t1", status="error")
    # Same trace_id, different run_id — should resume in place.
    await history.begin_trace(
        principal=alice, trace_id="t1", conversation_id="c1", run_id="r2"
    )
    bundle = await history.get_trace_bundle(principal=alice, trace_id="t1")
    assert bundle is not None
    assert bundle["trace"]["run_id"] == "r2"
    assert bundle["trace"]["status"] == "running"


@pytest.mark.asyncio
async def test_begin_trace_for_other_users_trace_raises_permission_error(
    history: SqliteHistoryStore,
    alice: UserPrincipal,
    bob: UserPrincipal,
) -> None:
    """Resuming a trace belonging to another user is rejected."""
    await history.upsert_user(alice)
    await history.upsert_user(bob)
    await history.begin_trace(
        principal=alice, trace_id="t1", conversation_id=None, run_id="r1"
    )
    with pytest.raises(PermissionError, match="another user"):
        await history.begin_trace(
            principal=bob, trace_id="t1", conversation_id=None, run_id="r1"
        )


def test_apply_sqlite_pragmas_skips_non_sqlite_url() -> None:
    """``_apply_sqlite_pragmas`` is a no-op for non-SQLite URLs.

    The WAL/busy_timeout listener only makes sense for SQLite; for a
    Postgres URL the function must return before touching the engine.
    """
    from openbb_agent_server.persistence.sqlite_store import _apply_sqlite_pragmas

    class _BoomEngine:
        @property
        def sync_engine(self) -> Any:
            raise AssertionError("engine must not be touched for non-sqlite URLs")

    _apply_sqlite_pragmas(_BoomEngine(), "postgresql+asyncpg://host/db")
