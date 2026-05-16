"""WidgetDataStore + parse_widget_data_messages tests."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from langchain_core.embeddings import Embeddings

from openbb_agent_server.memory.embeddings import HashEmbeddings
from openbb_agent_server.persistence import models as m
from openbb_agent_server.runtime.principal import UserPrincipal
from openbb_agent_server.runtime.widget_store import (
    WidgetDataStore,
    _extract_columns,
    _extract_rows,
    _extract_rows_from_dict,
    _row_text,
    _slugify_table_name,
    parse_widget_data_messages,
)


def _principal(user_id: str = "u") -> UserPrincipal:
    return UserPrincipal(user_id=user_id)


@pytest_asyncio.fixture
async def store() -> AsyncIterator[WidgetDataStore]:
    s = WidgetDataStore("sqlite+aiosqlite:///:memory:")
    async with s._engine.begin() as conn:
        await conn.run_sync(m.Base.metadata.create_all)
    yield s
    await s._engine.dispose()


@pytest_asyncio.fixture
async def store_with_embeddings(
    tmp_path: pytest.TempPathFactory,
) -> AsyncIterator[WidgetDataStore]:
    db = tmp_path / "widget.db"
    s = WidgetDataStore(
        f"sqlite+aiosqlite:///{db}",
        embeddings=HashEmbeddings(dim=64),
    )
    async with s._engine.begin() as conn:
        await conn.run_sync(m.Base.metadata.create_all)
    yield s
    await s._engine.dispose()


def test_slugify_empty_yields_default() -> None:
    assert _slugify_table_name("") == "widget"


def test_slugify_whitespace_yields_default() -> None:
    assert _slugify_table_name("   ") == "widget"


def test_slugify_alphanumeric_passes_through() -> None:
    assert _slugify_table_name("Foo_Bar 9") == "foo_bar_9"


def test_slugify_strips_unsafe_chars() -> None:
    assert _slugify_table_name('a"b/c.d') == "a_b_c_d"


def test_slugify_prefixes_leading_digit() -> None:
    assert _slugify_table_name("9foo") == "_9foo"


def test_slugify_strips_leading_trailing_underscores() -> None:
    assert _slugify_table_name("---abc---") == "abc"


def test_row_text_joins_keys() -> None:
    assert _row_text({"a": 1, "b": "x"}) == "a: 1 | b: x"


def test_row_text_skips_none() -> None:
    assert _row_text({"a": 1, "b": None, "c": "ok"}) == "a: 1 | c: ok"


def test_row_text_empty_dict() -> None:
    assert _row_text({}) == ""


@pytest.mark.asyncio
async def test_record_stores_basic_row(store: WidgetDataStore) -> None:
    rid = await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="market",
        input_args={"sym": "AAPL"},
        rows=[{"a": 1}, {"a": 2}],
        columns=["a"],
    )
    assert rid > 0
    entries = await store.list_entries(principal=_principal(), conversation_id="c1")
    assert len(entries) == 1
    assert entries[0]["row_count"] == 2
    assert entries[0]["widget_name"] == "Prices"
    assert entries[0]["ingested_at"]


@pytest.mark.asyncio
async def test_record_with_embeddings_indexes_rows_for_search(
    store_with_embeddings: WidgetDataStore,
) -> None:
    """``record`` writes rows to both the SQL table and the SQLiteVec index."""
    rid = await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="P",
        origin="o",
        input_args={},
        rows=[{"x": "apple"}, {"x": "banana"}],
        columns=["x"],
    )
    assert rid > 0
    latest = await store_with_embeddings.read_latest(
        principal=_principal(), conversation_id="c1"
    )
    assert latest is not None
    # ANN-backed search now finds the apple row by semantic match.
    hits = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c1", query="apple"
    )
    assert hits and hits[0]["row"]["x"] == "apple"


@pytest.mark.asyncio
async def test_record_vector_index_failure_still_persists(
    store_with_embeddings: WidgetDataStore,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken embedder doesn't block the row-level SQL insert."""

    class _Broken(Embeddings):
        def embed_documents(self, texts: list[str]) -> list[list[float]]:
            raise RuntimeError("nope")

        def embed_query(self, text: str) -> list[float]:
            raise RuntimeError("nope")

    monkeypatch.setattr(
        store_with_embeddings._vec, "embedding", _Broken(), raising=False
    )

    def boom(*_a: object, **_kw: object) -> None:
        raise RuntimeError("vec index down")

    monkeypatch.setattr(store_with_embeddings._vec, "add_texts", boom)
    rid = await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="w",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    # Indexing runs in the background so the SQL row commits first;
    # wait for it to finish before asserting on its log line.
    await store_with_embeddings.await_pending_indexing()
    assert rid > 0
    assert any("vector index failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_record_skips_embeddings_when_no_rows(
    store_with_embeddings: WidgetDataStore,
) -> None:
    rid = await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name=None,
        origin=None,
        input_args={},
        rows=[],
        columns=None,
    )
    assert rid > 0


@pytest.mark.asyncio
async def test_list_entries_isolated_per_user(store: WidgetDataStore) -> None:
    await store.record(
        principal=_principal("a"),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    await store.record(
        principal=_principal("b"),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": 2}],
        columns=["x"],
    )

    a_entries = await store.list_entries(
        principal=_principal("a"), conversation_id="c1"
    )
    b_entries = await store.list_entries(
        principal=_principal("b"), conversation_id="c1"
    )
    assert len(a_entries) == 1
    assert len(b_entries) == 1


@pytest.mark.asyncio
async def test_list_entries_empty(store: WidgetDataStore) -> None:
    out = await store.list_entries(principal=_principal(), conversation_id="cx")
    assert out == []


@pytest.mark.asyncio
async def test_list_entries_spans_conversations_for_user(
    store: WidgetDataStore,
) -> None:
    """``conversation_id=None`` returns every ingest the user has."""
    await store.record(
        principal=_principal("u"),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    await store.record(
        principal=_principal("u"),
        conversation_id="c2",
        widget_uuid="w-b",
        widget_name="B",
        origin="o",
        input_args={},
        rows=[{"x": 2}],
        columns=["x"],
    )
    await store.record(
        principal=_principal("other"),
        conversation_id="c1",
        widget_uuid="w-c",
        widget_name="C",
        origin="o",
        input_args={},
        rows=[{"x": 3}],
        columns=["x"],
    )
    out = await store.list_entries(principal=_principal("u"), conversation_id=None)
    uuids = sorted(e["widget_uuid"] for e in out)
    assert uuids == ["w-a", "w-b"]


@pytest.mark.asyncio
async def test_list_entries_default_spans_conversations(
    store: WidgetDataStore,
) -> None:
    """``list_entries(principal=…)`` (no conversation_id kw) defaults to cross-convo."""
    await store.record(
        principal=_principal("u"),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    await store.record(
        principal=_principal("u"),
        conversation_id="c2",
        widget_uuid="w-b",
        widget_name="B",
        origin="o",
        input_args={},
        rows=[{"x": 2}],
        columns=["x"],
    )
    out = await store.list_entries(principal=_principal("u"))
    assert {e["widget_uuid"] for e in out} == {"w-a", "w-b"}


@pytest.mark.asyncio
async def test_read_latest_returns_none_when_missing(store: WidgetDataStore) -> None:
    assert (
        await store.read_latest(principal=_principal(), conversation_id="empty") is None
    )


@pytest.mark.asyncio
async def test_read_latest_cross_conversation(store: WidgetDataStore) -> None:
    """``conversation_id=None`` finds data ingested in any prior conversation."""
    await store.record(
        principal=_principal("u"),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": "old"}],
        columns=["x"],
    )
    out = await store.read_latest(
        principal=_principal("u"),
        conversation_id=None,
        widget_uuid="w-1",
    )
    assert out is not None
    assert out["rows"] == [{"x": "old"}]


@pytest.mark.asyncio
async def test_read_latest_cross_conversation_still_isolates_users(
    store: WidgetDataStore,
) -> None:
    """Cross-conversation lookup must NEVER leak across users."""
    await store.record(
        principal=_principal("alice"),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": "alice-data"}],
        columns=["x"],
    )
    out = await store.read_latest(
        principal=_principal("bob"),
        conversation_id=None,
        widget_uuid="w-1",
    )
    assert out is None


@pytest.mark.asyncio
async def test_read_latest_returns_newest(store: WidgetDataStore) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={"v": 1},
        rows=[{"x": "first"}],
        columns=["x"],
    )
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={"v": 2},
        rows=[{"x": "second"}],
        columns=["x"],
    )
    out = await store.read_latest(principal=_principal(), conversation_id="c1")
    assert out is not None
    assert out["rows"] == [{"x": "second"}]


@pytest.mark.asyncio
async def test_read_latest_filters_by_widget_uuid(store: WidgetDataStore) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"x": "a"}],
        columns=["x"],
    )
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-b",
        widget_name="B",
        origin="o",
        input_args={},
        rows=[{"x": "b"}],
        columns=["x"],
    )
    out = await store.read_latest(
        principal=_principal(), conversation_id="c1", widget_uuid="w-a"
    )
    assert out is not None
    assert out["rows"] == [{"x": "a"}]


@pytest.mark.asyncio
async def test_read_latest_widget_name_falls_through_search(
    store: WidgetDataStore,
) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-b",
        widget_name="B",
        origin="o",
        input_args={},
        rows=[{"x": 2}],
        columns=["x"],
    )
    out = await store.read_latest(
        principal=_principal(), conversation_id="c1", widget_name="A"
    )
    assert out is not None
    assert out["widget_name"] == "A"


@pytest.mark.asyncio
async def test_read_latest_widget_name_fallthrough_returns_none(
    store: WidgetDataStore,
) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    out = await store.read_latest(
        principal=_principal(), conversation_id="c1", widget_name="Nonexistent"
    )
    assert out is None


@pytest.mark.asyncio
async def test_read_latest_max_rows_truncates(store: WidgetDataStore) -> None:
    rows = [{"x": i} for i in range(10)]
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=rows,
        columns=["x"],
    )
    out = await store.read_latest(
        principal=_principal(), conversation_id="c1", max_rows=3
    )
    assert out is not None
    assert len(out["rows"]) == 3


@pytest.mark.asyncio
async def test_search_empty_returns_empty(store: WidgetDataStore) -> None:
    out = await store.search(principal=_principal(), conversation_id="cx", query="foo")
    assert out == []


@pytest.mark.asyncio
async def test_search_substring_fallback_when_no_embeddings(
    store: WidgetDataStore,
) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}, {"city": "Rome"}, {"city": "Berlin"}],
        columns=["city"],
    )
    out = await store.search(
        principal=_principal(), conversation_id="c1", query="paris"
    )
    assert len(out) == 1
    assert out[0]["row"] == {"city": "Paris"}


@pytest.mark.asyncio
async def test_search_filters_by_widget_uuid(store: WidgetDataStore) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"v": "paris"}],
        columns=["v"],
    )
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-b",
        widget_name="B",
        origin="o",
        input_args={},
        rows=[{"v": "paris"}],
        columns=["v"],
    )
    out = await store.search(
        principal=_principal(),
        conversation_id="c1",
        query="paris",
        widget_uuid="w-a",
    )
    assert len(out) == 1


@pytest.mark.asyncio
async def test_search_uses_embeddings_when_available(
    store_with_embeddings: WidgetDataStore,
) -> None:
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[
            {"text": "apple fruit"},
            {"text": "banana fruit"},
            {"text": "completely unrelated"},
        ],
        columns=["text"],
    )
    out = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c1", query="apple", k=2
    )
    assert len(out) > 0


@pytest.mark.asyncio
async def test_search_returns_empty_for_blank_query(
    store: WidgetDataStore,
) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    out = await store.search(principal=_principal(), conversation_id="c1", query="")
    assert out == []


@pytest.mark.asyncio
async def test_ann_search_skips_other_users(
    store_with_embeddings: WidgetDataStore,
) -> None:
    """An ANN hit whose metadata belongs to a different user is dropped."""
    await store_with_embeddings.record(
        principal=_principal("a"),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    out = await store_with_embeddings.search(
        principal=_principal("b"), conversation_id="c1", query="Paris"
    )
    assert out == []


@pytest.mark.asyncio
async def test_ann_search_skips_other_conversations(
    store_with_embeddings: WidgetDataStore,
) -> None:
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    out = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c2", query="Paris"
    )
    assert out == []


@pytest.mark.asyncio
async def test_ann_search_spans_conversations_when_unscoped(
    store_with_embeddings: WidgetDataStore,
) -> None:
    """``conversation_id=None`` lets ANN return hits from any conversation."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    out = await store_with_embeddings.search(
        principal=_principal(), conversation_id=None, query="Paris"
    )
    assert len(out) == 1
    assert out[0]["row"] == {"city": "Paris"}


@pytest.mark.asyncio
async def test_substring_search_spans_conversations_when_unscoped(
    store: WidgetDataStore,
) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="old-convo",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    out = await store.search(
        principal=_principal(), conversation_id=None, query="paris"
    )
    assert len(out) == 1
    assert out[0]["row"] == {"city": "Paris"}


@pytest.mark.asyncio
async def test_ann_search_skips_mismatched_widget_uuid(
    store_with_embeddings: WidgetDataStore,
) -> None:
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    out = await store_with_embeddings.search(
        principal=_principal(),
        conversation_id="c1",
        query="Paris",
        widget_uuid="w-b",
    )
    assert out == []


@pytest.mark.asyncio
async def test_ann_search_dedupes_duplicate_hits(
    store_with_embeddings: WidgetDataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the ANN returns the same row twice, only one entry survives."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )

    real = store_with_embeddings._vec.similarity_search_with_score

    def dup(*a: object, **kw: object) -> list[Any]:
        out = real(*a, **kw)
        return out + out

    monkeypatch.setattr(store_with_embeddings._vec, "similarity_search_with_score", dup)
    hits = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c1", query="Paris"
    )
    assert len(hits) == 1


@pytest.mark.asyncio
async def test_fetch_row_missing_parent_returns_none(
    store_with_embeddings: WidgetDataStore,
) -> None:
    out = await store_with_embeddings._fetch_row(parent_id=99999, row_idx=0)
    assert out is None


@pytest.mark.asyncio
async def test_ann_search_skips_when_fetch_row_returns_none(
    store_with_embeddings: WidgetDataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the vec index points at a row that's since been pruned, drop it."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )

    async def fake_fetch(self: object, *, parent_id: int, row_idx: int) -> None:
        return None

    monkeypatch.setattr(WidgetDataStore, "_fetch_row", fake_fetch)
    out = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c1", query="Paris"
    )
    # Vec hit got dropped because the row was unfetchable; substring
    # fallback then runs and DOES find it via SQL — so we expect 1.
    assert len(out) == 1


@pytest.mark.asyncio
async def test_fetch_row_out_of_bounds_returns_none(
    store_with_embeddings: WidgetDataStore,
) -> None:
    rid = await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    assert await store_with_embeddings._fetch_row(parent_id=rid, row_idx=99) is None


@pytest.mark.asyncio
async def test_substring_search_returns_only_k_results(
    store: WidgetDataStore,
) -> None:
    """The substring path exits early after collecting ``k`` matches."""
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"v": "match"}] * 10,
        columns=["v"],
    )
    out = await store.search(
        principal=_principal(), conversation_id="c1", query="match", k=3
    )
    assert len(out) == 3


def test_url_to_file_handles_memory_path() -> None:
    from openbb_agent_server.runtime.widget_store import _url_to_file

    assert _url_to_file("sqlite+aiosqlite:///:memory:") is None
    assert _url_to_file("sqlite+aiosqlite:///") is None
    assert _url_to_file("/abs/path/x.db") == "/abs/path/x.db"
    assert _url_to_file("sqlite+aiosqlite:////real/path.db") == "/real/path.db"


@pytest.mark.asyncio
async def test_search_ann_failure_falls_back_to_substring(
    store_with_embeddings: WidgetDataStore,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ANN raises, the search falls back to the substring path."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )

    def boom(*_a: object, **_kw: object) -> None:
        raise RuntimeError("ann down")

    monkeypatch.setattr(
        store_with_embeddings._vec, "similarity_search_with_score", boom
    )
    out = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c1", query="Paris"
    )
    assert len(out) == 1
    assert any("ANN failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_schema_spans_conversations_when_unscoped(
    store: WidgetDataStore,
) -> None:
    """``conversation_id=None`` exposes widgets from every conversation."""
    await store.record(
        principal=_principal("u"),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"a": 1}],
        columns=["a"],
    )
    await store.record(
        principal=_principal("u"),
        conversation_id="c2",
        widget_uuid="w-b",
        widget_name="Volumes",
        origin="o",
        input_args={},
        rows=[{"b": 9}],
        columns=["b"],
    )
    sch = await store.schema(principal=_principal("u"), conversation_id=None)
    tables = {e["table"] for e in sch}
    assert tables == {"prices", "volumes"}


@pytest.mark.asyncio
async def test_query_spans_conversations_when_unscoped(
    store: WidgetDataStore,
) -> None:
    """SQL queries can see widgets ingested in other conversations."""
    await store.record(
        principal=_principal("u"),
        conversation_id="c-old",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"sym": "AAPL"}, {"sym": "GOOG"}],
        columns=["sym"],
    )
    out = await store.query(
        principal=_principal("u"),
        conversation_id=None,
        sql='SELECT "sym" FROM "prices"',
    )
    assert out["table_count"] == 1
    assert {r["sym"] for r in out["rows"]} == {"AAPL", "GOOG"}


@pytest.mark.asyncio
async def test_query_unscoped_still_isolates_users(store: WidgetDataStore) -> None:
    """Cross-conversation does not mean cross-user — alice's data is
    not visible to bob even when his ``conversation_id`` is None.
    """
    await store.record(
        principal=_principal("alice"),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"sym": "AAPL"}],
        columns=["sym"],
    )
    # Bob has zero widgets ingested, so the "prices" view is never
    # created for him and SQLite errors. The important assertion is
    # that bob does NOT see alice's rows.
    import sqlalchemy.exc

    with pytest.raises(sqlalchemy.exc.OperationalError):
        await store.query(
            principal=_principal("bob"),
            conversation_id=None,
            sql='SELECT "sym" FROM "prices"',
        )


@pytest.mark.asyncio
async def test_schema_dedupes_by_slug(store: WidgetDataStore) -> None:
    # Same widget_name → same slug → only latest survives.
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"a": 1}],
        columns=["a"],
    )
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"a": 2}, {"a": 3}],
        columns=["a"],
    )
    sch = await store.schema(principal=_principal(), conversation_id="c1")
    assert len(sch) == 1
    assert sch[0]["table"] == "prices"
    assert sch[0]["row_count"] == 2


@pytest.mark.asyncio
async def test_schema_falls_back_to_uuid_and_id(store: WidgetDataStore) -> None:
    rid = await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name=None,
        origin=None,
        input_args={},
        rows=[],
        columns=[],
    )
    sch = await store.schema(principal=_principal(), conversation_id="c1")
    assert len(sch) == 1
    # Slug derives from widget_uuid since widget_name is None.
    assert sch[0]["table"] == "w_1"
    assert sch[0]["row_count"] == 0
    assert rid > 0


@pytest.mark.asyncio
async def test_schema_uses_widget_id_when_uuid_blank(store: WidgetDataStore) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="",
        widget_name=None,
        origin=None,
        input_args={},
        rows=[],
        columns=[],
    )
    sch = await store.schema(principal=_principal(), conversation_id="c1")
    # Slug derives from "widget_{id}" when both widget_name and widget_uuid blank.
    assert sch[0]["table"].startswith("widget_")


@pytest.mark.asyncio
async def test_validate_query_sql_rejects_empty() -> None:
    with pytest.raises(ValueError, match="empty"):
        WidgetDataStore._validate_query_sql("   ")


@pytest.mark.asyncio
async def test_validate_query_sql_rejects_non_select() -> None:
    with pytest.raises(ValueError, match="SELECT / WITH"):
        WidgetDataStore._validate_query_sql("INSERT INTO foo VALUES (1)")


def test_validate_query_sql_accepts_select() -> None:
    assert WidgetDataStore._validate_query_sql("SELECT 1") == "SELECT 1"


def test_validate_query_sql_strips_trailing_semicolon() -> None:
    assert WidgetDataStore._validate_query_sql("SELECT 1;") == "SELECT 1"


def test_validate_query_sql_accepts_with() -> None:
    sql = "WITH x AS (SELECT 1) SELECT * FROM x"
    assert WidgetDataStore._validate_query_sql(sql) == sql


def test_resolve_view_columns_uses_explicit_columns() -> None:
    ing = m.WidgetData(
        user_id="u",
        conversation_id="c",
        widget_uuid="w",
        widget_name="n",
        origin="o",
        input_args={},
        columns=["a", "b"],
        rows=[{"a": 1, "c": 9}],
    )
    assert WidgetDataStore._resolve_view_columns(ing) == ["a", "b"]


def test_resolve_view_columns_infers_from_rows_when_no_columns() -> None:
    ing = m.WidgetData(
        user_id="u",
        conversation_id="c",
        widget_uuid="w",
        widget_name="n",
        origin="o",
        input_args={},
        columns=None,
        rows=[{"a": 1, "b": 2}],
    )
    assert WidgetDataStore._resolve_view_columns(ing) == ["a", "b"]


def test_resolve_view_columns_empty_when_no_rows_no_columns() -> None:
    ing = m.WidgetData(
        user_id="u",
        conversation_id="c",
        widget_uuid="w",
        widget_name="n",
        origin="o",
        input_args={},
        columns=None,
        rows=[],
    )
    assert WidgetDataStore._resolve_view_columns(ing) == []


def test_resolve_view_columns_skips_non_dict_rows() -> None:
    ing = m.WidgetData(
        user_id="u",
        conversation_id="c",
        widget_uuid="w",
        widget_name="n",
        origin="o",
        input_args={},
        columns=None,
        rows=[{"x": 1}, "not a dict", {"y": 2}],
    )
    out = WidgetDataStore._resolve_view_columns(ing)
    assert out == ["x", "y"]


def test_build_view_sql_empty_columns_makes_empty_view() -> None:
    sql = WidgetDataStore._build_view_sql("foo", [], 1)
    assert 'CREATE TEMP VIEW "foo"' in sql
    assert "WHERE 0" in sql


def test_build_view_sql_quotes_column_names() -> None:
    sql = WidgetDataStore._build_view_sql("t", ['a"b', "c"], 9)
    assert 'AS "a""b"' in sql
    assert "wd.id = 9" in sql


@pytest.mark.asyncio
async def test_query_rejects_postgres_dialect() -> None:
    store = WidgetDataStore("sqlite+aiosqlite:///:memory:")

    class _FakeDialect:
        name = "postgresql"

    store._engine.dialect = _FakeDialect()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="not supported"):
        await store.query(
            principal=_principal(),
            conversation_id="c1",
            sql="SELECT 1",
        )


@pytest.mark.asyncio
async def test_query_runs_basic_select(store: WidgetDataStore) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Prices",
        origin="o",
        input_args={},
        rows=[{"sym": "AAPL", "v": 100}, {"sym": "GOOG", "v": 200}],
        columns=["sym", "v"],
    )
    out = await store.query(
        principal=_principal(),
        conversation_id="c1",
        sql='SELECT "sym" FROM "prices" ORDER BY "v" DESC',
    )
    assert out["columns"] == ["sym"]
    assert out["rows"] == [{"sym": "GOOG"}, {"sym": "AAPL"}]
    assert out["truncated"] is False
    assert out["table_count"] == 1


@pytest.mark.asyncio
async def test_query_truncates_to_max_rows(store: WidgetDataStore) -> None:
    rows = [{"i": i} for i in range(20)]
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="Nums",
        origin="o",
        input_args={},
        rows=rows,
        columns=["i"],
    )
    out = await store.query(
        principal=_principal(),
        conversation_id="c1",
        sql='SELECT "i" FROM "nums"',
        max_rows=5,
    )
    assert len(out["rows"]) == 5
    assert out["truncated"] is True


@pytest.mark.asyncio
async def test_query_creates_view_per_widget(store: WidgetDataStore) -> None:
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"x": 1}],
        columns=["x"],
    )
    await store.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-b",
        widget_name="B",
        origin="o",
        input_args={},
        rows=[{"y": 9}],
        columns=["y"],
    )
    out = await store.query(
        principal=_principal(),
        conversation_id="c1",
        sql='SELECT "a"."x" AS x, "b"."y" AS y FROM "a", "b"',
    )
    assert out["table_count"] == 2
    assert out["rows"] == [{"x": 1, "y": 9}]


def test_extract_rows_handles_none() -> None:
    assert _extract_rows(None) == []


def test_extract_rows_handles_scalars() -> None:
    assert _extract_rows(42) == []
    assert _extract_rows(1.5) == []
    assert _extract_rows(True) == []


def test_extract_rows_recurses_list() -> None:
    assert _extract_rows([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]


def test_extract_rows_parses_json_string() -> None:
    assert _extract_rows('[{"a": 1}]') == [{"a": 1}]


def test_extract_rows_unparseable_string_returns_empty() -> None:
    assert _extract_rows("not json") == []


def test_extract_rows_unknown_type_returns_empty() -> None:
    class _Mystery:
        pass

    assert _extract_rows(_Mystery()) == []


def test_extract_rows_from_items_key() -> None:
    payload = {"items": [{"x": 1}, {"x": 2}]}
    assert _extract_rows(payload) == [{"x": 1}, {"x": 2}]


def test_extract_rows_from_content_key() -> None:
    payload = {"content": [{"x": 1}]}
    assert _extract_rows(payload) == [{"x": 1}]


def test_extract_rows_scalar_dict_becomes_single_row() -> None:
    payload = {"sym": "AAPL", "v": 100}
    assert _extract_rows(payload) == [{"sym": "AAPL", "v": 100}]


def test_extract_rows_nested_dict_returns_empty() -> None:
    payload = {"meta": {"nested": "value"}, "x": 1}
    assert _extract_rows(payload) == []


def test_extract_rows_from_dict_passes_through_to_helper() -> None:
    assert _extract_rows_from_dict({"items": [{"x": 1}]}) == [{"x": 1}]


def test_extract_columns_empty_rows() -> None:
    assert _extract_columns([]) is None


def test_extract_columns_unions_keys_in_order() -> None:
    assert _extract_columns([{"a": 1, "b": 2}, {"b": 3, "c": 4}]) == ["a", "b", "c"]


def test_extract_columns_skips_non_dict_rows() -> None:
    assert _extract_columns([{"a": 1}, "bad", {"b": 2}]) == ["a", "b"]


def test_extract_columns_all_non_dict_rows_returns_none() -> None:
    assert _extract_columns([None, "x"]) is None  # type: ignore[arg-type]


class _AIMessage:
    def __init__(self, content: str) -> None:
        self.role = "ai"
        self.content = content


class _ToolMessage:
    def __init__(self, data: Any) -> None:
        self.role = "tool"
        self.data = data


class _HumanMessage:
    def __init__(self, content: str) -> None:
        self.role = "human"
        self.content = content


class _StructuredAIMessage:
    """AI tool-call envelope using structured ``function`` /
    ``input_arguments`` fields (the modern wire shape)."""

    def __init__(self, function: str, input_arguments: dict[str, Any]) -> None:
        self.role = "ai"
        self.content = None
        self.function = function
        self.input_arguments = input_arguments


class _ToolMessageWithArgs:
    """Tool message that carries its own ``function`` / ``input_arguments``."""

    def __init__(
        self,
        data: Any,
        *,
        function: str | None = None,
        input_arguments: dict[str, Any] | None = None,
    ) -> None:
        self.role = "tool"
        self.data = data
        self.function = function
        self.input_arguments = input_arguments or {}


def test_parse_widget_data_messages_reads_structured_ai_envelope() -> None:
    """Wire-protocol primary path: AI envelope is structured fields,
    not JSON in content. ``widget_uuid`` must still flow through."""
    ai = _StructuredAIMessage(
        function="get_widget_data",
        input_arguments={
            "data_sources": [
                {
                    "widget_uuid": "uuid-1",
                    "origin": "blackrock",
                    "id": "blk_alloc_currency",
                    "input_args": {"ticker": "IEFA"},
                }
            ]
        },
    )
    tool = _ToolMessage([{"items": [{"label": "USD", "weight_pct": 0.5}]}])
    out = parse_widget_data_messages([ai, tool])
    assert len(out) == 1
    assert out[0]["widget_uuid"] == "uuid-1"
    assert out[0]["widget_name"] == "blk_alloc_currency"
    assert out[0]["rows"] == [{"label": "USD", "weight_pct": 0.5}]


def test_parse_widget_data_messages_reads_data_sources_from_tool_msg() -> None:
    """Some clients put ``data_sources`` on the tool message itself."""
    tool = _ToolMessageWithArgs(
        [{"items": [{"x": 1}]}],
        function="get_widget_data",
        input_arguments={
            "data_sources": [{"widget_uuid": "uuid-2", "id": "blk_alloc_sector"}]
        },
    )
    out = parse_widget_data_messages([tool])
    assert len(out) == 1
    assert out[0]["widget_uuid"] == "uuid-2"
    assert out[0]["widget_name"] == "blk_alloc_sector"


def test_parse_widget_data_messages_synthesises_from_widget_ids() -> None:
    """Wire fallback: the agent's tool signature is
    ``get_widget_data(widget_ids=[...])``. If the client forwards
    that verbatim without expanding to ``data_sources``, synthesise
    minimal sources so the rows still get stored under a usable
    ``widget_uuid``.
    """
    ai = _StructuredAIMessage(
        function="get_widget_data",
        input_arguments={"widget_ids": ["uuid-a", "uuid-b"]},
    )
    tool = _ToolMessage([{"items": [{"x": 1}]}, {"items": [{"y": 2}]}])
    out = parse_widget_data_messages([ai, tool])
    assert [i["widget_uuid"] for i in out] == ["uuid-a", "uuid-b"]
    assert out[0]["rows"] == [{"x": 1}]
    assert out[1]["rows"] == [{"y": 2}]


def test_parse_widget_data_messages_pairs_ai_envelope_with_tool_payload() -> None:
    envelope = {
        "function": "get_widget_data",
        "input_arguments": {
            "data_sources": [
                {
                    "widget_uuid": "w-1",
                    "origin": "market",
                    "id": "prices",
                    "input_args": {"sym": "AAPL"},
                }
            ]
        },
    }
    tool_payload = [{"items": [{"sym": "AAPL", "v": 100}]}]
    out = parse_widget_data_messages(
        [
            _AIMessage(json.dumps(envelope)),
            _ToolMessage(tool_payload),
        ]
    )
    assert len(out) == 1
    [ingest] = out
    assert ingest["widget_uuid"] == "w-1"
    assert ingest["widget_name"] == "prices"
    assert ingest["origin"] == "market"
    assert ingest["input_args"] == {"sym": "AAPL"}
    assert ingest["rows"] == [{"sym": "AAPL", "v": 100}]
    assert ingest["columns"] == ["sym", "v"]


def test_parse_widget_data_messages_handles_multiple_data_sources() -> None:
    envelope = {
        "function": "get_widget_data",
        "input_arguments": {
            "data_sources": [
                {"widget_uuid": "w-a", "id": "a", "input_args": {}},
                {"widget_uuid": "w-b", "id": "b", "input_args": {}},
            ]
        },
    }
    payload = [
        {"items": [{"x": 1}]},
        {"items": [{"x": 2}]},
    ]
    out = parse_widget_data_messages(
        [_AIMessage(json.dumps(envelope)), _ToolMessage(payload)]
    )
    assert [i["widget_name"] for i in out] == ["a", "b"]


def test_parse_widget_data_messages_skips_when_no_envelope() -> None:
    out = parse_widget_data_messages([_ToolMessage([{"items": [{"x": 1}]}])])
    assert out == []


def test_parse_widget_data_messages_human_role_resets_envelope() -> None:
    envelope = {"function": "get_widget_data", "input_arguments": {"data_sources": []}}
    out = parse_widget_data_messages(
        [
            _AIMessage(json.dumps(envelope)),
            _HumanMessage("excuse me"),
            _ToolMessage([{"items": [{"x": 1}]}]),
        ]
    )
    assert out == []


def test_parse_widget_data_messages_ignores_non_json_ai_content() -> None:
    out = parse_widget_data_messages(
        [
            _AIMessage("just chatting"),
            _ToolMessage([{"items": [{"x": 1}]}]),
        ]
    )
    assert out == []


def test_parse_widget_data_messages_ignores_unparseable_json() -> None:
    out = parse_widget_data_messages(
        [
            _AIMessage("{ not valid"),
            _ToolMessage([{"items": [{"x": 1}]}]),
        ]
    )
    assert out == []


def test_parse_widget_data_messages_ignores_wrong_function() -> None:
    bad_envelope = {"function": "something_else", "input_arguments": {}}
    out = parse_widget_data_messages(
        [
            _AIMessage(json.dumps(bad_envelope)),
            _ToolMessage([{"items": [{"x": 1}]}]),
        ]
    )
    assert out == []


def test_parse_widget_data_messages_skips_non_dict_data_sources() -> None:
    envelope = {
        "function": "get_widget_data",
        "input_arguments": {"data_sources": ["not a dict", {"widget_uuid": "ok"}]},
    }
    out = parse_widget_data_messages(
        [
            _AIMessage(json.dumps(envelope)),
            _ToolMessage([{"items": []}, {"items": [{"x": 1}]}]),
        ]
    )
    assert len(out) == 1
    assert out[0]["widget_uuid"] == "ok"


def test_parse_widget_data_messages_pads_when_payload_shorter() -> None:
    envelope = {
        "function": "get_widget_data",
        "input_arguments": {
            "data_sources": [
                {"widget_uuid": "w-1", "id": "a", "input_args": {}},
                {"widget_uuid": "w-2", "id": "b", "input_args": {}},
            ]
        },
    }
    out = parse_widget_data_messages(
        [
            _AIMessage(json.dumps(envelope)),
            _ToolMessage([{"items": [{"x": 1}]}]),
        ]
    )
    # Second data source has no payload — should still produce a record (with empty rows).
    assert len(out) == 2
    assert out[0]["rows"] == [{"x": 1}]
    assert out[1]["rows"] == []


def test_parse_widget_data_messages_normalises_non_list_data() -> None:
    envelope = {
        "function": "get_widget_data",
        "input_arguments": {
            "data_sources": [{"widget_uuid": "w-1", "id": "a", "input_args": {}}]
        },
    }
    # data is a single dict, not a list — gets wrapped.
    out = parse_widget_data_messages(
        [
            _AIMessage(json.dumps(envelope)),
            _ToolMessage({"items": [{"x": 1}]}),
        ]
    )
    assert len(out) == 1
    assert out[0]["rows"] == [{"x": 1}]


def test_parse_widget_data_messages_skips_role_none() -> None:
    """A message without a recognisable role gets the envelope reset."""

    class _Bare:
        role = None
        content = "x"

    envelope = {"function": "get_widget_data", "input_arguments": {"data_sources": []}}
    out = parse_widget_data_messages(
        [
            _AIMessage(json.dumps(envelope)),
            _Bare(),
            _ToolMessage([{"items": [{"x": 1}]}]),
        ]
    )
    assert out == []


def test_apply_sqlite_pragmas_skips_non_sqlite_url() -> None:
    """``_apply_sqlite_pragmas`` returns early for non-SQLite URLs."""
    from openbb_agent_server.runtime.widget_store import _apply_sqlite_pragmas

    class _BoomEngine:
        @property
        def sync_engine(self) -> Any:
            raise AssertionError("engine must not be touched for non-sqlite URLs")

    _apply_sqlite_pragmas(_BoomEngine(), "postgresql+asyncpg://host/db")


def test_index_rows_sync_noops_when_vec_unset(store: WidgetDataStore) -> None:
    """``_index_rows_sync`` returns immediately when there is no vec index."""
    # ``store`` is built without embeddings, so ``_vec`` is None — the
    # method must early-return rather than touch a missing index.
    assert store._vec is None
    store._index_rows_sync(1, "u", "c1", "w", "W", [{"x": 1}])


@pytest.mark.asyncio
async def test_await_pending_indexing_noop_when_no_tasks(
    store: WidgetDataStore,
) -> None:
    """``await_pending_indexing`` returns at once when nothing is in flight."""
    assert not store._indexing_tasks
    await store.await_pending_indexing()


@pytest.mark.asyncio
async def test_record_uses_inline_sync_index_when_no_running_loop(
    store_with_embeddings: WidgetDataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``record`` falls back to inline indexing when no loop is running.

    ``asyncio.get_running_loop`` normally succeeds inside an awaited
    coroutine; stubbing it to raise ``RuntimeError`` drives the sync
    branch that production hits only when ``record`` is called outside
    an event loop.
    """
    import asyncio as _asyncio

    calls: list[tuple[Any, ...]] = []
    real_sync = store_with_embeddings._index_rows_sync

    def _spy_sync(*args: Any) -> None:
        calls.append(args)
        real_sync(*args)

    def _no_loop() -> Any:
        raise RuntimeError("no running event loop")

    monkeypatch.setattr(
        "openbb_agent_server.runtime.widget_store.asyncio.get_running_loop",
        _no_loop,
    )
    monkeypatch.setattr(store_with_embeddings, "_index_rows_sync", _spy_sync)
    rid = await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    assert rid > 0
    # The inline sync indexer ran instead of a background task.
    assert calls
    del _asyncio


@pytest.mark.asyncio
async def test_record_inline_sync_index_failure_is_swallowed(
    store_with_embeddings: WidgetDataStore,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An inline-index failure on the no-loop path is logged, not raised."""

    def _no_loop() -> Any:
        raise RuntimeError("no running event loop")

    def _boom(*_a: Any) -> None:
        raise RuntimeError("inline index down")

    monkeypatch.setattr(
        "openbb_agent_server.runtime.widget_store.asyncio.get_running_loop",
        _no_loop,
    )
    monkeypatch.setattr(store_with_embeddings, "_index_rows_sync", _boom)
    rid = await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-1",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    assert rid > 0
    assert any("vector index failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_ann_search_skips_other_user_after_indexing(
    store_with_embeddings: WidgetDataStore,
) -> None:
    """A populated ANN index drops hits whose metadata user differs."""
    await store_with_embeddings.record(
        principal=_principal("a"),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    await store_with_embeddings.await_pending_indexing()
    out = await store_with_embeddings.search(
        principal=_principal("b"), conversation_id="c1", query="Paris"
    )
    assert out == []


@pytest.mark.asyncio
async def test_ann_search_skips_other_conversation_after_indexing(
    store_with_embeddings: WidgetDataStore,
) -> None:
    """A populated ANN index drops hits scoped to a different conversation."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    await store_with_embeddings.await_pending_indexing()
    out = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c2", query="Paris"
    )
    assert out == []


@pytest.mark.asyncio
async def test_ann_search_skips_mismatched_widget_uuid_after_indexing(
    store_with_embeddings: WidgetDataStore,
) -> None:
    """A populated ANN index drops hits for a non-matching widget_uuid."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w-a",
        widget_name="A",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    await store_with_embeddings.await_pending_indexing()
    out = await store_with_embeddings.search(
        principal=_principal(),
        conversation_id="c1",
        query="Paris",
        widget_uuid="w-b",
    )
    assert out == []


@pytest.mark.asyncio
async def test_ann_search_dedupes_duplicate_hits_after_indexing(
    store_with_embeddings: WidgetDataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A populated ANN index returning a row twice yields one entry."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    await store_with_embeddings.await_pending_indexing()
    real = store_with_embeddings._vec.similarity_search_with_score

    def _dup(*a: object, **kw: object) -> list[Any]:
        out = real(*a, **kw)
        return out + out

    monkeypatch.setattr(
        store_with_embeddings._vec, "similarity_search_with_score", _dup
    )
    hits = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c1", query="Paris"
    )
    assert len(hits) == 1


@pytest.mark.asyncio
async def test_ann_search_skips_unfetchable_row_after_indexing(
    store_with_embeddings: WidgetDataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A populated ANN hit pointing at a pruned row is dropped."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": "Paris"}],
        columns=["city"],
    )
    await store_with_embeddings.await_pending_indexing()

    async def _none_row(self: object, *, parent_id: int, row_idx: int) -> None:
        return None

    monkeypatch.setattr(WidgetDataStore, "_ann_search", WidgetDataStore._ann_search)
    monkeypatch.setattr(WidgetDataStore, "_fetch_row", _none_row)
    # ANN hit resolves but the row is unfetchable, so the ANN result
    # set is empty and search falls back to substring (which still
    # finds the row via SQL).
    out = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c1", query="Paris"
    )
    assert len(out) == 1


@pytest.mark.asyncio
async def test_ann_search_stops_at_k_results_after_indexing(
    store_with_embeddings: WidgetDataStore,
) -> None:
    """The ANN result loop breaks once ``k`` rows have been collected."""
    await store_with_embeddings.record(
        principal=_principal(),
        conversation_id="c1",
        widget_uuid="w",
        widget_name="W",
        origin="o",
        input_args={},
        rows=[{"city": c} for c in ("Paris", "Parma", "Parana", "Parkes")],
        columns=["city"],
    )
    await store_with_embeddings.await_pending_indexing()
    hits = await store_with_embeddings.search(
        principal=_principal(), conversation_id="c1", query="Par", k=2
    )
    assert len(hits) == 2
