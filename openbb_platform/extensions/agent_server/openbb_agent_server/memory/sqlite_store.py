"""SQLite-backed :class:`MemoryStore` using ``SQLiteVec``."""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import re
import secrets
import sqlite3
import threading
from typing import Any

import sqlite_vec
from langchain_community.vectorstores import SQLiteVec
from langchain_core.embeddings import Embeddings

from openbb_agent_server.memory.embeddings import HashEmbeddings
from openbb_agent_server.memory.reranker import NvidiaReranker
from openbb_agent_server.memory.store import Memory, MemoryStore
from openbb_agent_server.runtime.principal import UserPrincipal

_URL_RE = re.compile(r"^sqlite(?:\+\w+)?:///(?P<path>.*)$")

_TEXT_TABLE = "memories_text"
_CODE_TABLE = "memories_code"


def _url_to_file(url: str) -> str:
    """Convert a SQLAlchemy ``sqlite[+driver]:///<path>`` URL to a file path."""
    m = _URL_RE.match(url)
    if not m:
        return url
    path = m.group("path")
    return path if path else ":memory:"


def _build_connection(db_file: str) -> sqlite3.Connection:
    """SQLite connection usable from any thread (the asyncio executor pool)."""
    conn = sqlite3.connect(db_file, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    return conn


def _is_code_kind(kind: str | None) -> bool:
    """Return True when the kind routes to the code-embedded table."""
    return bool(kind) and kind.endswith("_code")


class SqliteMemoryStore(MemoryStore):
    """Per-user vector memory backed by ``langchain_community.vectorstores.SQLiteVec``."""

    def __init__(
        self,
        url: str,
        *,
        embeddings: Embeddings | None = None,
        code_embeddings: Embeddings | None = None,
        reranker: NvidiaReranker | None = None,
        rerank_fanout: int = 32,
    ) -> None:
        self._db_file = _url_to_file(url)
        self._embeddings: Embeddings = embeddings or HashEmbeddings()
        self._code_embeddings = code_embeddings
        self._reranker = reranker
        self._rerank_fanout = max(1, int(rerank_fanout))
        self._conn = _build_connection(self._db_file)
        self._lock = threading.Lock()
        self._text = SQLiteVec(
            table=_TEXT_TABLE,
            connection=self._conn,
            embedding=self._embeddings,
            db_file=self._db_file,
        )
        self._text.create_table_if_not_exists()
        self._code: SQLiteVec | None = None
        if self._code_embeddings is not None:
            self._code = SQLiteVec(
                table=_CODE_TABLE,
                connection=self._conn,
                embedding=self._code_embeddings,
                db_file=self._db_file,
            )
            self._code.create_table_if_not_exists()

    def _store_for_kind(self, kind: str | None) -> SQLiteVec:
        if _is_code_kind(kind) and self._code is not None:
            return self._code
        return self._text

    def _all_tables(self) -> list[str]:
        return [_TEXT_TABLE, _CODE_TABLE] if self._code is not None else [_TEXT_TABLE]

    async def write(
        self,
        *,
        principal: UserPrincipal,
        text: str,
        kind: str = "fact",
        source_trace_id: str | None = None,
    ) -> Memory:
        if not principal.has_scope("memory:write"):
            raise PermissionError("memory:write scope required")
        memory_id = secrets.token_urlsafe(12)
        meta: dict[str, Any] = {
            "memory_id": memory_id,
            "user_id": principal.user_id,
            "kind": kind,
            "pinned": False,
            "source_trace_id": source_trace_id,
            "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        }
        store = self._store_for_kind(kind)

        def _add() -> None:
            with self._lock:
                store.add_texts([text], metadatas=[meta])

        await asyncio.to_thread(_add)
        return Memory(
            memory_id=memory_id,
            user_id=principal.user_id,
            text=text,
            kind=kind,
            source_trace_id=source_trace_id,
        )

    async def recall(
        self,
        *,
        principal: UserPrincipal,
        query: str,
        k: int = 8,
    ) -> list[Memory]:
        fanout = max(k, self._rerank_fanout)

        def _gather() -> tuple[list[tuple[Memory, float]], list[Memory]]:
            pool: list[tuple[Memory, float]] = []
            with self._lock:
                for table, store in self._iter_stores():
                    pool.extend(
                        _ann_search(
                            table=table,
                            store=store,
                            query=query,
                            user_id=principal.user_id,
                            fanout=fanout,
                        )
                    )
                pinned_rows = _pinned_rows(
                    conn=self._conn,
                    tables=self._all_tables(),
                    user_id=principal.user_id,
                )
            return pool, pinned_rows

        ann_pool, pinned = await asyncio.to_thread(_gather)
        merged = _merge_with_pinned(ann_pool, pinned)
        merged.sort(key=lambda x: x[1], reverse=True)

        if self._reranker is None:
            return [_with_score(mem, score) for mem, score in merged[:k]]

        rerank_pool = merged[:fanout]
        seen = {m.memory_id for m, _ in rerank_pool}
        for mem, score in merged[fanout:]:
            if mem.pinned and mem.memory_id not in seen:
                rerank_pool.append((mem, score))
                seen.add(mem.memory_id)

        candidates = [(mem.memory_id, mem.text) for mem, _ in rerank_pool]
        try:
            ranked = await self._reranker.rerank(query, candidates, top_k=k)
        except Exception:
            return [_with_score(mem, score) for mem, score in merged[:k]]
        by_id = {mem.memory_id: mem for mem, _ in rerank_pool}
        return [_with_score(by_id[mid], score) for mid, score in ranked if mid in by_id]

    async def list_memories(
        self,
        *,
        principal: UserPrincipal,
        limit: int = 100,
    ) -> list[Memory]:
        def _run() -> list[Memory]:
            with self._lock:
                return _list_by_user(
                    conn=self._conn,
                    tables=self._all_tables(),
                    user_id=principal.user_id,
                    limit=limit,
                )

        return await asyncio.to_thread(_run)

    async def pin(
        self,
        *,
        principal: UserPrincipal,
        memory_id: str,
        pinned: bool,
    ) -> Memory | None:
        def _run() -> Memory | None:
            with self._lock:
                return _set_pinned(
                    conn=self._conn,
                    tables=self._all_tables(),
                    user_id=principal.user_id,
                    memory_id=memory_id,
                    pinned=pinned,
                )

        return await asyncio.to_thread(_run)

    async def forget(
        self,
        *,
        principal: UserPrincipal,
        memory_id: str,
    ) -> bool:
        def _run() -> bool:
            with self._lock:
                return _forget_one(
                    conn=self._conn,
                    tables=self._all_tables(),
                    user_id=principal.user_id,
                    memory_id=memory_id,
                )

        return await asyncio.to_thread(_run)

    async def delete_all_for_user(self, principal: UserPrincipal) -> int:
        def _run() -> int:
            with self._lock:
                return delete_all_for_user(
                    self._conn, self._all_tables(), principal.user_id
                )

        return await asyncio.to_thread(_run)

    def _iter_stores(self) -> list[tuple[str, SQLiteVec]]:
        out = [(_TEXT_TABLE, self._text)]
        if self._code is not None:
            out.append((_CODE_TABLE, self._code))
        return out


def _ann_search(
    *,
    table: str,
    store: SQLiteVec,
    query: str,
    user_id: str,
    fanout: int,
) -> list[tuple[Memory, float]]:
    """Run ANN search filtered by user_id."""
    candidates = store.similarity_search_with_score(query, k=fanout * 4)
    out: list[tuple[Memory, float]] = []
    for doc, distance in candidates:
        meta = dict(doc.metadata or {})
        if meta.get("user_id") != user_id:
            continue
        out.append((_doc_to_memory(table, doc, meta), _distance_to_score(distance)))
        if len(out) >= fanout:
            break
    return out


def _pinned_rows(*, conn: Any, tables: list[str], user_id: str) -> list[Memory]:
    out: list[Memory] = []
    for table in tables:
        sql = (
            f"SELECT text, metadata FROM {table} "  # noqa: S608 - table name is a literal
            "WHERE json_extract(metadata, '$.user_id') = ? "
            "AND json_extract(metadata, '$.pinned') = 1"
        )
        for row in conn.execute(sql, (user_id,)).fetchall():
            meta = json.loads(row["metadata"]) or {}
            out.append(_row_to_memory(table, row["text"], meta))
    return out


def _list_by_user(
    *, conn: Any, tables: list[str], user_id: str, limit: int
) -> list[Memory]:
    rows: list[tuple[str, str, dict[str, Any]]] = []
    for table in tables:
        sql = (
            f"SELECT text, metadata FROM {table} "  # noqa: S608
            "WHERE json_extract(metadata, '$.user_id') = ?"
        )
        for row in conn.execute(sql, (user_id,)).fetchall():
            meta = json.loads(row["metadata"]) or {}
            rows.append((table, row["text"], meta))
    rows.sort(key=lambda r: r[2].get("created_at") or "", reverse=True)
    return [_row_to_memory(t, text, meta) for t, text, meta in rows[:limit]]


def _set_pinned(
    *,
    conn: Any,
    tables: list[str],
    user_id: str,
    memory_id: str,
    pinned: bool,
) -> Memory | None:
    for table in tables:
        sql_select = (
            f"SELECT rowid, text, metadata FROM {table} "  # noqa: S608
            "WHERE json_extract(metadata, '$.memory_id') = ?"
        )
        row = conn.execute(sql_select, (memory_id,)).fetchone()
        if row is None:
            continue
        meta = json.loads(row["metadata"]) or {}
        if meta.get("user_id") != user_id:
            return None
        meta["pinned"] = bool(pinned)
        sql_update = f"UPDATE {table} SET metadata = ? WHERE rowid = ?"  # noqa: S608
        conn.execute(sql_update, (json.dumps(meta), row["rowid"]))
        conn.commit()
        return _row_to_memory(table, row["text"], meta)
    return None


def _forget_one(
    *,
    conn: Any,
    tables: list[str],
    user_id: str,
    memory_id: str,
) -> bool:
    for table in tables:
        sql_select = (
            f"SELECT rowid, metadata FROM {table} "  # noqa: S608
            "WHERE json_extract(metadata, '$.memory_id') = ?"
        )
        row = conn.execute(sql_select, (memory_id,)).fetchone()
        if row is None:
            continue
        meta = json.loads(row["metadata"]) or {}
        if meta.get("user_id") != user_id:
            return False
        conn.execute(
            f"DELETE FROM {table} WHERE rowid = ?",  # noqa: S608
            (row["rowid"],),
        )
        conn.execute(
            f"DELETE FROM {table}_vec WHERE rowid = ?",  # noqa: S608
            (row["rowid"],),
        )
        conn.commit()
        return True
    return False


def delete_all_for_user(conn: Any, tables: list[str], user_id: str) -> int:
    """Drop every row in the given tables for the user."""
    deleted = 0
    for table in tables:
        sql_select = (
            f"SELECT rowid FROM {table} "  # noqa: S608
            "WHERE json_extract(metadata, '$.user_id') = ?"
        )
        rowids = [r["rowid"] for r in conn.execute(sql_select, (user_id,)).fetchall()]
        for rid in rowids:
            conn.execute(f"DELETE FROM {table} WHERE rowid = ?", (rid,))  # noqa: S608
            conn.execute(
                f"DELETE FROM {table}_vec WHERE rowid = ?",  # noqa: S608
                (rid,),
            )
            deleted += 1
        conn.commit()
    return deleted


def _row_to_memory(table: str, text: str, meta: dict[str, Any]) -> Memory:
    return Memory(
        memory_id=str(meta.get("memory_id") or ""),
        user_id=str(meta.get("user_id") or ""),
        text=text,
        kind=str(meta.get("kind") or "fact"),
        pinned=bool(meta.get("pinned", False)),
        source_trace_id=meta.get("source_trace_id"),
    )


def _doc_to_memory(table: str, doc: Any, meta: dict[str, Any]) -> Memory:
    return _row_to_memory(table, doc.page_content, meta)


def _distance_to_score(distance: float) -> float:
    """Map a distance to a similarity score in [0, 1]."""
    return 1.0 / (1.0 + max(0.0, float(distance)))


def _merge_with_pinned(
    ann: list[tuple[Memory, float]], pinned: list[Memory]
) -> list[tuple[Memory, float]]:
    by_id: dict[str, tuple[Memory, float]] = {
        mem.memory_id: (mem, score) for mem, score in ann
    }
    for mem in pinned:
        existing = by_id.get(mem.memory_id)
        score = max(existing[1] if existing else 0.0, 1.0)
        by_id[mem.memory_id] = (mem, score)
    return list(by_id.values())


def _with_score(mem: Memory, score: float) -> Memory:
    return mem.model_copy(update={"score": score})
