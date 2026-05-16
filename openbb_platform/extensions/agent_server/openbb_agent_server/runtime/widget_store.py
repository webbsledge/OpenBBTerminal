"""Widget-data store."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
import threading
from collections.abc import Sequence
from typing import Any

import sqlite_vec
from langchain_community.vectorstores import SQLiteVec
from langchain_core.embeddings import Embeddings
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from openbb_agent_server.persistence import models as m
from openbb_agent_server.runtime.principal import UserPrincipal

logger = logging.getLogger("openbb_agent_server.runtime.widget_store")


_TABLE_NAME_RE = re.compile(r"[^a-zA-Z0-9_]+")
_URL_RE = re.compile(r"^sqlite(?:\+\w+)?:///(?P<path>.*)$")
_VEC_TABLE = "widget_rows_vec"


def _slugify_table_name(name: str) -> str:
    """Map a widget name to a safe SQLite identifier."""
    slug = _TABLE_NAME_RE.sub("_", (name or "").strip()).strip("_").lower()
    if not slug:
        slug = "widget"
    if slug[0].isdigit():
        slug = f"_{slug}"
    return slug


def _row_text(row: dict[str, Any]) -> str:
    """Flatten a row dict into a single text string for embedding / search."""
    parts: list[str] = []
    for k, v in row.items():
        if v is None:
            continue
        parts.append(f"{k}: {v}")
    return " | ".join(parts)


def _url_to_file(url: str) -> str | None:
    """Return the file path for a SQLite URL, or ``None`` for ``:memory:``."""
    m = _URL_RE.match(url)
    if not m:
        return url
    path = m.group("path")
    if not path or path == ":memory:":
        return None
    return path


def _build_vec_connection(db_file: str) -> sqlite3.Connection:
    """Open a thread-safe sqlite3 connection with the ``sqlite-vec`` extension."""
    conn = sqlite3.connect(db_file, check_same_thread=False, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    # WAL + busy_timeout so this sync connection plays nice with the
    # async aiosqlite engine writing to the same file.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _apply_sqlite_pragmas(engine: AsyncEngine, url: str) -> None:
    """Enable WAL + busy_timeout on a SQLite async engine."""
    if "sqlite" not in url:
        return

    @event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_conn: Any, _: Any) -> None:
        cur = dbapi_conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA busy_timeout=5000")
            cur.execute("PRAGMA synchronous=NORMAL")
        finally:
            cur.close()


class WidgetDataStore:
    """SQL-backed widget-data store with optional ANN row search via SQLiteVec."""

    def __init__(
        self,
        url: str,
        *,
        embeddings: Embeddings | None = None,
        engine: AsyncEngine | None = None,
    ) -> None:
        self._engine = engine or create_async_engine(url, future=True)
        if engine is None:
            _apply_sqlite_pragmas(self._engine, url)
        self._sessionmaker = async_sessionmaker(
            self._engine,
            expire_on_commit=False,
        )
        self._embeddings = embeddings
        self._vec: SQLiteVec | None = None
        self._vec_conn: sqlite3.Connection | None = None
        self._vec_lock = threading.Lock()
        # Background indexing tasks — production fire-and-forget,
        # tests ``await await_pending_indexing()`` to flush.
        self._indexing_tasks: set[asyncio.Task[None]] = set()
        db_file = _url_to_file(url)
        if embeddings is not None and db_file is not None:
            self._vec_conn = _build_vec_connection(db_file)
            self._vec = SQLiteVec(
                table=_VEC_TABLE,
                connection=self._vec_conn,
                embedding=embeddings,
                db_file=db_file,
            )
            self._vec.create_table_if_not_exists()

    async def record(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str,
        widget_uuid: str,
        widget_name: str | None,
        origin: str | None,
        input_args: dict[str, Any],
        rows: list[dict[str, Any]],
        columns: list[str] | None,
    ) -> int:
        """Persist one ingestion. Returns the WidgetData row id.

        The canonical SQL row commits synchronously (so subsequent
        ``read_widget_data`` / ``query_widget_data`` calls see it).
        The vector index — which embeds every row and writes through
        a sync ``sqlite3`` connection that fights with our async
        engine — is launched as a fire-and-forget background task
        so a 10K-row widget doesn't stall the request. Semantic
        search falls back to substring match until the index catches
        up.
        """
        async with self._sessionmaker() as session:
            wd = m.WidgetData(
                user_id=principal.user_id,
                conversation_id=conversation_id,
                widget_uuid=widget_uuid,
                widget_name=widget_name,
                origin=origin,
                input_args=dict(input_args or {}),
                columns=list(columns or []),
                rows=list(rows),
            )
            session.add(wd)
            await session.commit()
            parent_id = int(wd.id)

        if self._vec is not None and rows:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            indexing_args = (
                parent_id,
                principal.user_id,
                conversation_id,
                widget_uuid,
                widget_name,
                list(rows),
            )
            if loop is not None:
                task = loop.create_task(self._index_in_background(*indexing_args))
                self._indexing_tasks.add(task)
                task.add_done_callback(self._indexing_tasks.discard)
            else:
                # Sync path (tests): run inline.
                try:
                    self._index_rows_sync(*indexing_args)
                except Exception:
                    logger.warning(
                        "widget_data.record: vector index failed; "
                        "search will fall back to substring match",
                        exc_info=True,
                    )
        return parent_id

    def _index_rows_sync(
        self,
        parent_id: int,
        user_id: str,
        conversation_id: str,
        widget_uuid: str,
        widget_name: str | None,
        rows: list[dict[str, Any]],
    ) -> None:
        """Write the vector index synchronously, in batches.

        ``SQLiteVec.add_texts`` runs through a sync sqlite3 cursor;
        batching keeps each transaction small so other connections
        can grab the WAL writer lock between batches and the event
        loop isn't starved on big widgets.
        """
        if self._vec is None or not rows:
            return
        batch_size = 256
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            texts = [_row_text(r) for r in batch]
            metas = [
                {
                    "parent_id": parent_id,
                    "row_idx": start + idx,
                    "user_id": user_id,
                    "conversation_id": conversation_id,
                    "widget_uuid": widget_uuid,
                    "widget_name": widget_name,
                }
                for idx in range(len(batch))
            ]
            with self._vec_lock:
                self._vec.add_texts(texts, metadatas=metas)

    async def await_pending_indexing(self) -> None:
        """Wait for any in-flight background indexing tasks to finish.

        Tests call this to make assertions about side-effects of
        ``record`` deterministic. Production code rarely needs it —
        indexing is fire-and-forget and queries handle the index
        being briefly behind.
        """
        if not self._indexing_tasks:
            return
        await asyncio.gather(*self._indexing_tasks, return_exceptions=True)

    async def _index_in_background(
        self,
        parent_id: int,
        user_id: str,
        conversation_id: str,
        widget_uuid: str,
        widget_name: str | None,
        rows: list[dict[str, Any]],
    ) -> None:
        """Run the sync indexing in a thread so the event loop stays free."""
        try:
            await asyncio.to_thread(
                self._index_rows_sync,
                parent_id,
                user_id,
                conversation_id,
                widget_uuid,
                widget_name,
                rows,
            )
        except Exception:
            logger.warning(
                "widget_data.record: background vector index failed for "
                "widget_uuid=%s (%d rows) — search falls back to substring",
                widget_uuid,
                len(rows),
                exc_info=True,
            )

    async def list_entries(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List widgets ingested for this user.

        ``conversation_id=None`` spans every conversation the user has
        — that's the path the agent uses to discover widget data that
        was fetched in a prior conversation. Pass an explicit
        ``conversation_id`` to scope to one conversation.
        """
        async with self._sessionmaker() as session:
            stmt = select(m.WidgetData).where(
                m.WidgetData.user_id == principal.user_id,
            )
            if conversation_id is not None:
                stmt = stmt.where(m.WidgetData.conversation_id == conversation_id)
            stmt = stmt.order_by(m.WidgetData.ingested_at.desc())
            rows = (await session.execute(stmt)).scalars().all()
        return [
            {
                "id": r.id,
                "widget_uuid": r.widget_uuid,
                "widget_name": r.widget_name,
                "origin": r.origin,
                "input_args": r.input_args,
                "columns": r.columns,
                "row_count": len(r.rows or []),
                "ingested_at": r.ingested_at.isoformat() if r.ingested_at else None,
            }
            for r in rows
        ]

    async def read_latest(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str | None = None,
        widget_uuid: str | None = None,
        widget_name: str | None = None,
        max_rows: int | None = None,
    ) -> dict[str, Any] | None:
        """Return the most recent ingest matching ``widget_uuid`` or
        ``widget_name``. ``conversation_id=None`` spans every
        conversation belonging to the user.
        """
        async with self._sessionmaker() as session:
            stmt = select(m.WidgetData).where(
                m.WidgetData.user_id == principal.user_id,
            )
            if conversation_id is not None:
                stmt = stmt.where(m.WidgetData.conversation_id == conversation_id)
            stmt = stmt.order_by(m.WidgetData.ingested_at.desc())
            if widget_uuid:
                stmt = stmt.where(m.WidgetData.widget_uuid == widget_uuid)
            row = (await session.execute(stmt)).scalars().first()
        if row is None:
            return None
        if (
            widget_name
            and row.widget_name
            and widget_name.strip().lower() != row.widget_name.strip().lower()
        ):
            # Found a row but wrong widget name — fall through to a
            # broader search.
            async with self._sessionmaker() as session:
                wn = widget_name.strip().lower()
                stmt2 = select(m.WidgetData).where(
                    m.WidgetData.user_id == principal.user_id,
                )
                if conversation_id is not None:
                    stmt2 = stmt2.where(m.WidgetData.conversation_id == conversation_id)
                stmt2 = stmt2.order_by(m.WidgetData.ingested_at.desc())
                candidates = (await session.execute(stmt2)).scalars().all()
            row = next(
                (c for c in candidates if (c.widget_name or "").strip().lower() == wn),
                None,
            )
            if row is None:
                return None

        clean_rows = list(row.rows or [])
        if max_rows is not None:
            clean_rows = clean_rows[: int(max_rows)]
        return {
            "id": row.id,
            "widget_uuid": row.widget_uuid,
            "widget_name": row.widget_name,
            "origin": row.origin,
            "input_args": row.input_args,
            "columns": row.columns,
            "rows": clean_rows,
            "ingested_at": row.ingested_at.isoformat() if row.ingested_at else None,
        }

    async def search(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str | None = None,
        query: str,
        k: int = 8,
        widget_uuid: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return the top-``k`` rows across the user's widget data.

        ``conversation_id=None`` spans every conversation for the user;
        pass an explicit id to scope to one conversation.
        """
        if not query.strip():
            return []
        if self._vec is not None:
            hits = await self._ann_search(
                principal=principal,
                conversation_id=conversation_id,
                query=query,
                k=k,
                widget_uuid=widget_uuid,
            )
            if hits:
                return hits
        return await self._substring_search(
            principal=principal,
            conversation_id=conversation_id,
            query=query,
            k=k,
            widget_uuid=widget_uuid,
        )

    async def _ann_search(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str | None,
        query: str,
        k: int,
        widget_uuid: str | None,
    ) -> list[dict[str, Any]]:
        import asyncio

        vec = self._vec
        if vec is None:  # pragma: no cover - caller gates on self._vec
            return []

        def _run() -> list[tuple[Any, float]]:
            with self._vec_lock:
                return vec.similarity_search_with_score(query, k=k * 4)

        try:
            scored = await asyncio.to_thread(_run)
        except Exception:
            logger.warning(
                "widget_data.search: ANN failed; falling back to substring",
                exc_info=True,
            )
            return []

        out: list[dict[str, Any]] = []
        seen: set[tuple[int, int]] = set()
        for doc, distance in scored:
            meta = dict(doc.metadata or {})
            if meta.get("user_id") != principal.user_id:
                continue
            if (
                conversation_id is not None
                and meta.get("conversation_id") != conversation_id
            ):
                continue
            if widget_uuid and meta.get("widget_uuid") != widget_uuid:
                continue
            key = (int(meta.get("parent_id") or 0), int(meta.get("row_idx") or 0))
            if key in seen:
                continue
            seen.add(key)
            row = await self._fetch_row(parent_id=key[0], row_idx=key[1])
            if row is None:
                continue
            out.append(
                {
                    "score": 1.0 / (1.0 + max(0.0, float(distance))),
                    "row": row,
                    "widget_uuid": meta.get("widget_uuid"),
                    "widget_name": meta.get("widget_name"),
                }
            )
            if len(out) >= k:
                break
        return out

    async def _substring_search(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str | None,
        query: str,
        k: int,
        widget_uuid: str | None,
    ) -> list[dict[str, Any]]:
        async with self._sessionmaker() as session:
            stmt = select(m.WidgetData).where(
                m.WidgetData.user_id == principal.user_id,
            )
            if conversation_id is not None:
                stmt = stmt.where(m.WidgetData.conversation_id == conversation_id)
            stmt = stmt.order_by(m.WidgetData.ingested_at.desc())
            if widget_uuid:
                stmt = stmt.where(m.WidgetData.widget_uuid == widget_uuid)
            ingests = (await session.execute(stmt)).scalars().all()

        q_lower = query.lower().strip()
        out: list[dict[str, Any]] = []
        for ing in ingests:
            for row in ing.rows or []:
                text = _row_text(row).lower()
                if q_lower and q_lower in text:
                    out.append(
                        {
                            "score": 1.0,
                            "row": row,
                            "widget_uuid": ing.widget_uuid,
                            "widget_name": ing.widget_name,
                        }
                    )
                    if len(out) >= k:
                        return out
        return out

    async def _fetch_row(
        self, *, parent_id: int, row_idx: int
    ) -> dict[str, Any] | None:
        async with self._sessionmaker() as session:
            wd = await session.get(m.WidgetData, parent_id)
        if wd is None:
            return None
        rows = wd.rows or []
        if row_idx < 0 or row_idx >= len(rows):
            return None
        return dict(rows[row_idx])

    async def schema(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return the table-name → columns mapping the agent should target.

        ``conversation_id=None`` spans every conversation for the user.
        """
        async with self._sessionmaker() as session:
            stmt = select(m.WidgetData).where(
                m.WidgetData.user_id == principal.user_id,
            )
            if conversation_id is not None:
                stmt = stmt.where(m.WidgetData.conversation_id == conversation_id)
            stmt = stmt.order_by(m.WidgetData.ingested_at.asc())
            ingests = (await session.execute(stmt)).scalars().all()
        out: list[dict[str, Any]] = []
        # Deduplicate by slug — keep the LATEST ingest per widget so the
        # table name is stable across turns. Iterate newest first.
        seen: set[str] = set()
        for ing in reversed(ingests):
            slug = _slugify_table_name(
                ing.widget_name or ing.widget_uuid or f"widget_{ing.id}"
            )
            if slug in seen:
                continue
            seen.add(slug)
            out.append(
                {
                    "table": slug,
                    "widget_name": ing.widget_name,
                    "widget_uuid": ing.widget_uuid,
                    "columns": list(ing.columns or []),
                    "row_count": len(ing.rows or []),
                }
            )
        out.reverse()
        return out

    @staticmethod
    def _validate_query_sql(sql: str) -> str:
        stripped = sql.strip().rstrip(";").strip()
        if not stripped:
            raise ValueError("empty SQL statement")
        first = stripped.split(None, 1)[0].lower()
        if first not in {"select", "with"}:
            raise ValueError(
                f"only SELECT / WITH statements are allowed; got {first.upper()!r}"
            )
        return stripped

    @staticmethod
    def _resolve_view_columns(ing: m.WidgetData) -> list[str]:
        cols = list(ing.columns or [])
        if cols or not ing.rows:
            return cols
        seen_keys: dict[str, None] = {}
        for r in ing.rows or []:
            if not isinstance(r, dict):
                continue
            for k in r:
                if k not in seen_keys:
                    seen_keys[k] = None
        return list(seen_keys)

    @staticmethod
    def _build_view_sql(slug: str, cols: list[str], ing_id: int) -> str:
        """Build a SQLite ``CREATE TEMP VIEW`` statement for one ingest."""
        if not cols:
            empty_view = f'CREATE TEMP VIEW "{slug}" ("__empty") AS SELECT NULL WHERE 0'  # noqa: S608
            return empty_view
        projections: list[str] = []
        for c in cols:
            json_path = '$."' + c.replace('"', '""') + '"'
            out_col = c.replace('"', '""')
            projections.append(
                f"json_extract(je.value, '{json_path}') AS \"{out_col}\""
            )
        proj_sql = ", ".join(projections)
        view_sql = (
            f'CREATE TEMP VIEW "{slug}" AS '  # noqa: S608
            f"SELECT {proj_sql} FROM widget_data wd, json_each(wd.rows) AS je "
            f"WHERE wd.id = {int(ing_id)}"
        )
        return view_sql

    async def query(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str | None = None,
        sql: str,
        max_rows: int = 500,
    ) -> dict[str, Any]:
        """Run a READ-ONLY SQL query against the ``widget_data`` SQL table.

        ``conversation_id=None`` exposes every widget the user has
        ingested (across conversations) as a temp view. Pass an
        explicit conversation_id to scope the SQL surface to one
        conversation.
        """
        stripped = self._validate_query_sql(sql)
        dialect = self._engine.dialect.name
        if dialect != "sqlite":
            # Postgres path would use ``jsonb_array_elements`` / a
            # different json_extract syntax. Out of scope for v1.
            raise RuntimeError(
                f"widget_store.query: dialect {dialect!r} not supported; SQLite only"
            )

        async with self._sessionmaker() as session:
            stmt = select(m.WidgetData).where(
                m.WidgetData.user_id == principal.user_id,
            )
            if conversation_id is not None:
                stmt = stmt.where(m.WidgetData.conversation_id == conversation_id)
            stmt = stmt.order_by(m.WidgetData.ingested_at.asc())
            ingests = (await session.execute(stmt)).scalars().all()

        latest: dict[str, m.WidgetData] = {}
        for ing in ingests:
            slug = _slugify_table_name(
                ing.widget_name or ing.widget_uuid or f"widget_{ing.id}"
            )
            latest[slug] = ing

        async with self._engine.begin() as conn:
            for slug, ing in latest.items():
                cols = self._resolve_view_columns(ing)
                view_sql = self._build_view_sql(slug, cols, int(ing.id))
                await conn.exec_driver_sql(view_sql)
            result = await conn.exec_driver_sql(stripped)
            keys = list(result.keys())
            fetched = result.fetchmany(max_rows + 1)
            truncated = len(fetched) > max_rows
            fetched = fetched[:max_rows]
            out_rows = [dict(zip(keys, row)) for row in fetched]

        return {
            "columns": keys,
            "rows": out_rows,
            "table_count": len(latest),
            "truncated": truncated,
        }


def _ai_envelope_from_message(msg: Any) -> dict[str, Any] | None:
    """Return the ``{"function": ..., "input_arguments": ...}`` envelope.

    Prefers the structured ``msg.function`` / ``msg.input_arguments``
    fields; falls back to JSON-in-content for older wire shims. Only
    returns the envelope when ``function == "get_widget_data"``.
    """
    fn = getattr(msg, "function", None)
    if fn == "get_widget_data":
        return {
            "function": fn,
            "input_arguments": dict(getattr(msg, "input_arguments", None) or {}),
        }
    content = getattr(msg, "content", None)
    if isinstance(content, str) and content.strip().startswith("{"):
        try:
            parsed = json.loads(content)
        except ValueError:
            return None
        if isinstance(parsed, dict) and parsed.get("function") == "get_widget_data":
            return parsed
    return None


def parse_widget_data_messages(
    body_messages: Sequence[Any],
) -> list[dict[str, Any]]:
    """Walk wire-protocol messages and pull out ``get_widget_data`` results.

    The Workspace wire protocol sends AI tool-call envelopes in TWO
    shapes:

    1. ``role:"ai"`` with structured ``function`` + ``input_arguments``
       fields at the top level (the modern path).
    2. ``role:"ai"`` with ``content`` set to a JSON string of the form
       ``{"function": ..., "input_arguments": ...}`` (the legacy
       wire-shim path some clients still emit).

    We accept either — missing the structured shape silently dropped
    every ingest's ``widget_uuid`` because the AI envelope was never
    paired with the following ``role:"tool"`` message.
    """
    ingests: list[dict[str, Any]] = []
    last_ai_envelope: dict[str, Any] | None = None
    for msg in body_messages:
        role = getattr(msg, "role", None)
        if role == "ai":
            envelope = _ai_envelope_from_message(msg)
            if envelope is not None:
                last_ai_envelope = envelope
            continue
        if role != "tool":
            last_ai_envelope = None
            continue

        # tool message — pair with last_ai_envelope (from the AI
        # turn) OR with the tool message's own ``input_arguments`` /
        # ``function`` if the wire client only sent the tool row.
        # Reset the AI envelope after pairing so a stray AI envelope
        # without a matching tool message doesn't bleed into the next
        # tool row.
        envelope = last_ai_envelope or _ai_envelope_from_message(msg)
        last_ai_envelope = None
        if not envelope:
            continue
        input_args_full = envelope.get("input_arguments") or {}
        data_sources = input_args_full.get("data_sources") or []
        if not data_sources:
            # Fall back to the tool message's own input_arguments —
            # some clients put the expanded ``data_sources`` here
            # rather than on the AI envelope.
            tool_args = getattr(msg, "input_arguments", None) or {}
            data_sources = tool_args.get("data_sources") or []
        if not data_sources:
            # Last-resort fallback: the agent's tool signature is
            # ``get_widget_data(widget_ids=[...])`` and the wire
            # client may forward that shape verbatim without
            # expanding to ``data_sources``. Synthesise a minimal
            # data source per id so the rows still get stored with
            # something matchable.
            for source in (
                input_args_full,
                getattr(msg, "input_arguments", None) or {},
            ):
                widget_ids = source.get("widget_ids") or []
                if widget_ids:
                    data_sources = [
                        {"widget_uuid": str(wid), "id": str(wid), "input_args": {}}
                        for wid in widget_ids
                        if wid
                    ]
                    break

        # Data payload arrives as ``msg.data = [{"items":[{"content":<json>}]}, ...]``
        # — one entry per data_source from the AI envelope. Pair them up.
        data_field = getattr(msg, "data", None) or []
        if not isinstance(data_field, list):
            data_field = [data_field]

        for i, ds in enumerate(data_sources):
            if not isinstance(ds, dict):
                continue
            widget_uuid = str(ds.get("widget_uuid") or "")
            origin = str(ds.get("origin") or "")
            widget_id = str(ds.get("id") or "")
            input_args = dict(ds.get("input_args") or {})

            payload = data_field[i] if i < len(data_field) else None
            rows = _extract_rows(payload)
            columns = _extract_columns(rows)
            ingests.append(
                {
                    "widget_uuid": widget_uuid,
                    "widget_name": widget_id or None,
                    "origin": origin or None,
                    "input_args": input_args,
                    "rows": rows,
                    "columns": columns,
                }
            )
        last_ai_envelope = None
    return ingests


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    """Best-effort: pull a list of row dicts out of a Workspace data payload."""
    if payload is None or isinstance(payload, (int, float, bool)):
        return []
    if isinstance(payload, list):
        rows: list[dict[str, Any]] = []
        for entry in payload:
            rows.extend(_extract_rows(entry))
        return rows
    if isinstance(payload, dict):
        return _extract_rows_from_dict(payload)
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except ValueError:
            return []
        return _extract_rows(parsed)
    return []


def _extract_rows_from_dict(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if "items" in payload:
        return _extract_rows(payload.get("items"))
    if "content" in payload:
        return _extract_rows(payload.get("content"))
    scalar_types = (str, int, float, bool, type(None))
    if all(isinstance(v, scalar_types) for v in payload.values()):
        return [dict(payload)]
    return []


def _extract_columns(rows: list[dict[str, Any]]) -> list[str] | None:
    """Union the keys of all rows, preserving first-seen order."""
    if not rows:
        return None
    seen: dict[str, None] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k in r:
            if k not in seen:
                seen[k] = None
    return list(seen.keys()) if seen else None
