"""Unit tests for ``inspect_widget_data`` tool source."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

from openbb_agent_server.persistence import models as m
from openbb_agent_server.plugins.tools.inspect_widget_data import (
    InspectWidgetDataToolSource,
)
from openbb_agent_server.runtime import services
from openbb_agent_server.runtime.context import RunContext, WidgetRef, bind
from openbb_agent_server.runtime.principal import UserPrincipal
from openbb_agent_server.runtime.widget_store import WidgetDataStore


def _principal() -> UserPrincipal:
    return UserPrincipal(user_id="alice")


def _ctx(widget_uuids: tuple[str, ...] = ("w-1",)) -> RunContext:
    return RunContext(
        principal=_principal(),
        trace_id="t",
        run_id="r",
        conversation_id="c1",
        widgets=tuple(WidgetRef(uuid=u) for u in widget_uuids),
    )


@pytest_asyncio.fixture
async def widget_store() -> AsyncIterator[WidgetDataStore]:
    s = WidgetDataStore("sqlite+aiosqlite:///:memory:")
    async with s._engine.begin() as conn:
        await conn.run_sync(m.Base.metadata.create_all)
    services.set_services(history=None, widget_store=s)
    try:
        yield s
    finally:
        services.reset()
        await s._engine.dispose()


async def _get_tool(name: str) -> Any:
    src = InspectWidgetDataToolSource()
    tools = await src.tools(_ctx(), {})
    [tool] = [t for t in tools if t.name == name]
    return tool


@pytest.mark.asyncio
async def test_list_widget_data_empty(widget_store: WidgetDataStore) -> None:
    """An empty store returns the ``{count, widgets, attached_widgets, message}`` envelope, not a bare list. NIM-class models reject empty tool results, and the envelope guides the agent toward ``get_widget_data`` instead of looping."""
    with bind(_ctx()):
        tool = await _get_tool("list_widget_data")
        out = await tool.ainvoke({})
    assert out["count"] == 0
    assert out["widgets"] == []
    assert "message" in out


@pytest.mark.asyncio
async def test_list_widget_data_returns_entries(
    widget_store: WidgetDataStore,
) -> None:
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    with bind(_ctx()):
        tool = await _get_tool("list_widget_data")
        out = await tool.ainvoke({})
    assert out["count"] == 1
    assert out["widgets"][0]["widget_uuid"] == "w-1"


@pytest.mark.asyncio
async def test_read_widget_data_returns_rows(
    widget_store: WidgetDataStore,
) -> None:
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": "a"}, {"x": "b"}],
        columns=["x"],
    )
    with bind(_ctx()):
        tool = await _get_tool("read_widget_data")
        out = await tool.ainvoke({"widget_uuid": "w-1"})
    assert out["rows"] == [{"x": "a"}, {"x": "b"}]


@pytest.mark.asyncio
async def test_read_widget_data_no_match(widget_store: WidgetDataStore) -> None:
    """An unknown widget_uuid returns ``None`` (no rows in store)."""
    with bind(_ctx()):
        tool = await _get_tool("read_widget_data")
        out = await tool.ainvoke({"widget_uuid": "missing"})
    assert out is None


@pytest.mark.asyncio
async def test_read_widget_data_reads_cross_conversation_orphan(
    widget_store: WidgetDataStore,
) -> None:
    """A widget stored in a prior conversation can still be read by
    its uuid even if it isn't currently pinned — the user owns the
    data; the citation filter handles UI noise separately.
    """
    await widget_store.record(
        principal=_principal(),
        conversation_id="other-convo",
        widget_uuid="orphan",
        widget_name="Orphan",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    with bind(_ctx()):
        tool = await _get_tool("read_widget_data")
        out = await tool.ainvoke({"widget_uuid": "orphan"})
    assert isinstance(out, dict)
    assert out["rows"] == [{"x": 1}]


@pytest.mark.asyncio
async def test_list_widget_data_returns_cross_conversation_entries(
    widget_store: WidgetDataStore,
) -> None:
    """``list_widget_data`` surfaces every widget the user has stored,
    not just the ones on the current dashboard."""
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="OnDash",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    await widget_store.record(
        principal=_principal(),
        conversation_id="old-convo",
        widget_uuid="orphan",
        widget_name="Orphan",
        origin="o",
        input_args={},
        rows=[{"x": 99}],
        columns=["x"],
    )
    with bind(_ctx()):
        tool = await _get_tool("list_widget_data")
        out = await tool.ainvoke({})
    assert out["count"] == 2
    uuids = sorted(e["widget_uuid"] for e in out["widgets"])
    assert uuids == ["orphan", "w-1"]


@pytest.mark.asyncio
async def test_describe_widget_data_returns_cross_conversation_entries(
    widget_store: WidgetDataStore,
) -> None:
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="OnDash",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    await widget_store.record(
        principal=_principal(),
        conversation_id="old-convo",
        widget_uuid="orphan",
        widget_name="Orphan",
        origin="o",
        input_args={},
        rows=[{"x": 9}],
        columns=["x"],
    )
    with bind(_ctx()):
        tool = await _get_tool("describe_widget_data")
        out = await tool.ainvoke({})
    assert len(out) == 2


@pytest.mark.asyncio
async def test_search_widget_data_returns_cross_conversation_hits(
    widget_store: WidgetDataStore,
) -> None:
    await widget_store.record(
        principal=_principal(),
        conversation_id="old-convo",
        widget_uuid="orphan",
        widget_name="Orphan",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    with bind(_ctx()):
        tool = await _get_tool("search_widget_data")
        out = await tool.ainvoke({"query": "paris"})
    assert len(out) == 1
    assert out[0]["row"] == {"city": "Paris"}


@pytest.mark.asyncio
async def test_search_widget_data_substring(
    widget_store: WidgetDataStore,
) -> None:
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}, {"city": "Rome"}],
        columns=["city"],
    )
    with bind(_ctx()):
        tool = await _get_tool("search_widget_data")
        out = await tool.ainvoke({"query": "paris", "k": 5})
    assert len(out) == 1
    assert out[0]["row"] == {"city": "Paris"}


@pytest.mark.asyncio
async def test_describe_widget_data(widget_store: WidgetDataStore) -> None:
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    with bind(_ctx()):
        tool = await _get_tool("describe_widget_data")
        out = await tool.ainvoke({})
    assert len(out) == 1
    assert out[0]["table"] == "prices"


@pytest.mark.asyncio
async def test_query_widget_data_success(
    widget_store: WidgetDataStore,
) -> None:
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"sym": "A", "v": "1"}, {"sym": "B", "v": "2"}],
        columns=["sym", "v"],
    )
    with bind(_ctx()):
        tool = await _get_tool("query_widget_data")
        out = await tool.ainvoke({"sql": 'SELECT "sym" FROM "prices"'})
    assert out["columns"] == ["sym"]
    assert [r["sym"] for r in out["rows"]] == ["A", "B"]


@pytest.mark.asyncio
async def test_query_widget_data_invalid_sql_returns_error(
    widget_store: WidgetDataStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with bind(_ctx()):
        tool = await _get_tool("query_widget_data")
        out = await tool.ainvoke({"sql": "DROP TABLE foo"})
    assert "error" in out
    assert out["rows"] == []
    assert out["columns"] == []
    assert any("query_widget_data failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_list_returns_empty_envelope_when_store_unbound() -> None:
    """Even with no store, the envelope shape is preserved so the agent gets actionable guidance instead of an empty list."""
    services.reset()
    with bind(_ctx()):
        tool = await _get_tool("list_widget_data")
        out = await tool.ainvoke({})
    assert out["count"] == 0
    assert out["widgets"] == []


@pytest.mark.asyncio
async def test_read_returns_none_when_store_unbound() -> None:
    services.reset()
    with bind(_ctx()):
        tool = await _get_tool("read_widget_data")
        out = await tool.ainvoke({"widget_uuid": "x"})
    assert out is None


@pytest.mark.asyncio
async def test_search_returns_empty_when_store_unbound() -> None:
    services.reset()
    with bind(_ctx()):
        tool = await _get_tool("search_widget_data")
        out = await tool.ainvoke({"query": "x"})
    assert out == []


@pytest.mark.asyncio
async def test_describe_returns_empty_when_store_unbound() -> None:
    services.reset()
    with bind(_ctx()):
        tool = await _get_tool("describe_widget_data")
        out = await tool.ainvoke({})
    assert out == []


@pytest.mark.asyncio
async def test_query_returns_error_when_store_unbound() -> None:
    services.reset()
    with bind(_ctx()):
        tool = await _get_tool("query_widget_data")
        out = await tool.ainvoke({"sql": "SELECT 1"})
    assert out["error"] == "widget store unavailable"
    assert out["columns"] == []
    assert out["rows"] == []


@pytest.mark.asyncio
async def test_list_widget_data_second_call_same_turn_short_circuits(
    widget_store: WidgetDataStore,
) -> None:
    """A second ``list_widget_data`` call in one turn returns a STOP message.

    ``list_widget_data`` is a pure index lookup — the per-run
    ``list_called`` guard makes the repeat call return the same data
    plus a hard-stop message so NIM-class models don't loop.
    """
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    src = InspectWidgetDataToolSource()
    tools = await src.tools(_ctx(), {})
    [tool] = [t for t in tools if t.name == "list_widget_data"]
    with bind(_ctx()):
        first = await tool.ainvoke({})
        second = await tool.ainvoke({})
    assert "message" not in first
    assert "already called this turn" in second["message"]


@pytest.mark.asyncio
async def test_read_widget_data_cites_each_widget_only_once(
    widget_store: WidgetDataStore,
) -> None:
    """Reading the same pinned widget twice emits a citation only once.

    ``_cite_widget`` records cited dashboard uuids in a per-run set —
    the second read sees the uuid already in ``cited`` and returns
    without re-emitting.
    """
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    emitted: list[dict[str, Any]] = []
    from openbb_agent_server.runtime import emit

    src = InspectWidgetDataToolSource()
    tools = await src.tools(_ctx(), {})
    [tool] = [t for t in tools if t.name == "read_widget_data"]
    with bind(_ctx()), emit.bind_writer(emitted.append):
        await tool.ainvoke({"widget_uuid": "w-1"})
        await tool.ainvoke({"widget_uuid": "w-1"})
    citations = [e for e in emitted if e.get("type") == "citations"]
    assert len(citations) == 1


@pytest.mark.asyncio
async def test_query_widget_data_cites_widgets_referenced_by_sql(
    widget_store: WidgetDataStore,
) -> None:
    """A successful query cites every pinned widget whose table the SQL names."""
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"sym": "A"}],
        columns=["sym"],
    )
    emitted: list[dict[str, Any]] = []
    from openbb_agent_server.runtime import emit

    src = InspectWidgetDataToolSource()
    tools = await src.tools(_ctx(), {})
    [tool] = [t for t in tools if t.name == "query_widget_data"]
    with bind(_ctx()), emit.bind_writer(emitted.append):
        out = await tool.ainvoke({"sql": 'SELECT "sym" FROM "prices"'})
    assert out["columns"] == ["sym"]
    citations = [e for e in emitted if e.get("type") == "citations"]
    assert len(citations) == 1


@pytest.mark.asyncio
async def test_query_widget_data_swallows_schema_failure_during_citation() -> None:
    """If ``schema`` raises while collecting citations, the query result stands.

    ``_cite_widgets_referenced_by_sql`` wraps its ``store.schema`` call
    in a broad ``except`` — a failure there must not fail the query.
    """

    class _FakeStore:
        async def query(self, **_: Any) -> dict[str, Any]:
            return {"columns": ["a"], "rows": [{"a": 1}], "table_count": 1}

        async def schema(self, **_: Any) -> list[dict[str, Any]]:
            raise RuntimeError("schema introspection blew up")

    services.set_services(history=None, widget_store=_FakeStore())
    try:
        src = InspectWidgetDataToolSource()
        tools = await src.tools(_ctx(), {})
        [tool] = [t for t in tools if t.name == "query_widget_data"]
        with bind(_ctx()):
            out = await tool.ainvoke({"sql": "SELECT a FROM prices"})
        assert out == {"columns": ["a"], "rows": [{"a": 1}], "table_count": 1}
    finally:
        services.reset()


@pytest.mark.asyncio
async def test_query_widget_data_citation_pass_handles_store_vanishing() -> None:
    """If the widget store disappears between query and the citation pass,
    ``_cite_widgets_referenced_by_sql`` re-resolves it, finds ``None``, and
    returns without touching the (now absent) store.
    """

    class _SelfClearingStore:
        async def query(self, **_: Any) -> dict[str, Any]:
            # The result is built; now the store is detached, so the
            # follow-up citation pass re-resolves to ``None``.
            services.reset()
            return {"columns": ["a"], "rows": [{"a": 1}], "table_count": 1}

    services.set_services(history=None, widget_store=_SelfClearingStore())
    try:
        src = InspectWidgetDataToolSource()
        tools = await src.tools(_ctx(), {})
        [tool] = [t for t in tools if t.name == "query_widget_data"]
        with bind(_ctx()):
            out = await tool.ainvoke({"sql": "SELECT a FROM prices"})
        assert out == {"columns": ["a"], "rows": [{"a": 1}], "table_count": 1}
    finally:
        services.reset()


@pytest.mark.asyncio
async def test_cite_widget_indexes_dashboard_widget_by_internal_id(
    widget_store: WidgetDataStore,
) -> None:
    """A pinned widget carrying a ``widget_id`` slug is indexed by that slug.

    When the stored entry's ``widget_uuid`` does not match a dashboard
    uuid, ``_cite_widget`` resolves it via the internal ``widget_id``
    index instead — so a citation still fires.
    """
    # Stored entry keyed by the internal widget_id slug (no per-instance
    # uuid match), but the pinned dashboard widget exposes that slug.
    await widget_store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="balance_slug",
        widget_name="balance_slug",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    ctx = RunContext(
        principal=_principal(),
        trace_id="t",
        run_id="r",
        conversation_id="c1",
        widgets=(WidgetRef(uuid="inst-uuid", widget_id="balance_slug"),),
    )
    emitted: list[dict[str, Any]] = []
    from openbb_agent_server.runtime import emit

    src = InspectWidgetDataToolSource()
    tools = await src.tools(ctx, {})
    [tool] = [t for t in tools if t.name == "read_widget_data"]
    with bind(ctx), emit.bind_writer(emitted.append):
        await tool.ainvoke({"widget_name": "balance_slug"})
    citations = [e for e in emitted if e.get("type") == "citations"]
    assert len(citations) == 1
    # Cited against the LIVE dashboard uuid, not the stored slug.
    src_info = citations[0]["citations"][0]["source_info"]
    assert src_info["uuid"] == "inst-uuid"
