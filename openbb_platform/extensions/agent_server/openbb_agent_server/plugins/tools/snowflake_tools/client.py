"""Snowflake connection client."""

from __future__ import annotations

import datetime as _dt
import logging
import secrets
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from openbb_agent_server.plugins.tools.snowflake_tools import safety

logger = logging.getLogger("openbb_agent_server.tools.snowflake.client")


class SnowflakeCredentials(BaseModel):
    """Connection credentials."""

    model_config = ConfigDict(extra="allow")

    account: str = ""
    user: str = ""
    password: str | None = None
    private_key: str | None = None
    private_key_passphrase: str | None = None
    authenticator: str = "snowflake"
    token: str | None = None
    role: str | None = None
    warehouse: str | None = None
    database: str | None = None
    schema_: str | None = Field(default=None, alias="schema")
    host: str | None = None
    region: str | None = None
    statement_timeout: int = 60
    network_timeout: int = 60

    def to_connect_kwargs(self) -> dict[str, Any]:
        """Build kwargs for :func:`snowflake.connector.connect`."""
        kwargs: dict[str, Any] = {}
        for k in (
            "account",
            "user",
            "role",
            "warehouse",
            "database",
            "host",
            "region",
            "authenticator",
            "token",
            "password",
        ):
            v = getattr(self, k)
            if v:
                kwargs[k] = v
        if self.schema_:
            kwargs["schema"] = self.schema_
        if self.private_key:
            kwargs["private_key"] = self._loaded_private_key()
        kwargs["client_session_keep_alive"] = False
        kwargs["network_timeout"] = self.network_timeout
        return kwargs

    def _loaded_private_key(self) -> bytes:
        from cryptography.hazmat.primitives import serialization

        raw = self.private_key or ""
        if "\n" not in raw and "/" in raw:
            from pathlib import Path

            raw = Path(raw).expanduser().read_text()
        passphrase = (
            self.private_key_passphrase.encode()
            if self.private_key_passphrase
            else None
        )
        priv = serialization.load_pem_private_key(
            raw.encode(),
            password=passphrase,
        )
        return priv.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )


class QueryResult(BaseModel):
    """One executed-statement result."""

    model_config = ConfigDict(extra="allow")

    sql: str
    statement_kind: str
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    query_id: str | None = None
    elapsed_ms: int | None = None
    warning: str | None = None


ConnectionFactory = Callable[[SnowflakeCredentials], Any]


SESSION_EXPIRED_CODES: frozenset[int] = frozenset({390111, 390112, 390114})


def _is_session_expired(exc: BaseException) -> bool:
    code = getattr(exc, "errno", None)
    if isinstance(code, int) and code in SESSION_EXPIRED_CODES:
        return True
    msg = str(exc)
    return any(str(c) in msg for c in SESSION_EXPIRED_CODES)


def _default_connection_factory(  # pragma: no cover — opens a live Snowflake connection
    creds: SnowflakeCredentials,
) -> Any:
    try:
        import snowflake.connector  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "Snowflake support requires snowflake-connector-python. "
            "Install the agent_server with the [snowflake] extra."
        ) from exc
    return snowflake.connector.connect(**creds.to_connect_kwargs())


class SnowflakeClient:
    """Thin, safe wrapper around a Snowflake connection."""

    def __init__(
        self,
        credentials: SnowflakeCredentials,
        *,
        connection_factory: ConnectionFactory | None = None,
        read_only: bool = True,
        max_rows: int = 10_000,
    ) -> None:
        self.credentials = credentials
        self._factory = connection_factory or _default_connection_factory
        self.read_only = read_only
        self.max_rows = max_rows
        self._conn: Any = None

    def open(self) -> None:
        if self._conn is None:
            self._conn = self._factory(self.credentials)
            self._apply_session_settings(self._conn)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # pragma: no cover — connector close errors
                logger.debug("snowflake connection close raised", exc_info=True)
            self._conn = None

    def __enter__(self) -> SnowflakeClient:
        self.open()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _apply_session_settings(self, conn: Any) -> None:
        try:
            cur = conn.cursor()
            cur.execute(
                f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {int(self.credentials.statement_timeout)}"
            )
            cur.close()
        except Exception:
            logger.debug("could not apply STATEMENT_TIMEOUT_IN_SECONDS", exc_info=True)

    def execute(  # noqa: PLR0912 — retry-loop + session-recovery branching.
        self,
        sql: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        max_rows: int | None = None,
    ) -> QueryResult:
        """Execute a single statement and return a :class:`QueryResult`."""
        if self.read_only:
            safety.enforce_read_only(sql)
        cap = max_rows if max_rows is not None else self.max_rows
        prepared = safety.inject_limit(sql, cap)

        if self._conn is None:
            self.open()
        if self._conn is None:  # pragma: no cover - open() raises on failure
            raise RuntimeError("snowflake connection not open after open()")

        kind = safety.classify(prepared)
        started = _dt.datetime.now(_dt.timezone.utc)
        try:
            cur = self._conn.cursor()
        except Exception as exc:
            if not _is_session_expired(exc):
                raise
            logger.warning("session expired on cursor open; reconnecting")
            self.close()
            self.open()
            if self._conn is None:  # pragma: no cover - open() raises on failure
                raise RuntimeError("snowflake connection not open after open()")
            cur = self._conn.cursor()
        try:
            try:
                if params is None:
                    cur.execute(prepared)
                else:
                    cur.execute(prepared, params)
            except Exception as exc:
                if not _is_session_expired(exc):
                    raise
                logger.warning(
                    "session expired during execute; reconnecting and retrying"
                )
                cur.close()
                self.close()
                self.open()
                if self._conn is None:  # pragma: no cover - open() raises on failure
                    raise RuntimeError("snowflake connection not open after open()")
                cur = self._conn.cursor()
                if params is None:
                    cur.execute(prepared)
                else:
                    cur.execute(prepared, params)
            description = cur.description or []
            columns = [
                d[0] if isinstance(d, (tuple, list)) else getattr(d, "name", str(d))
                for d in description
            ]
            rows = cur.fetchmany(cap + 1) if cap else cur.fetchall()
            truncated = bool(cap) and len(rows) > cap
            if truncated:
                rows = rows[:cap]
            query_id = (
                getattr(cur, "sfqid", None)
                or getattr(cur, "query_id", None)
                or f"local-{secrets.token_hex(6)}"
            )
        finally:
            cur.close()
        elapsed_ms = int(
            (_dt.datetime.now(_dt.timezone.utc) - started).total_seconds() * 1000
        )

        return QueryResult(
            sql=prepared,
            statement_kind=kind,
            columns=list(columns),
            rows=[list(r) for r in rows],
            row_count=len(rows),
            truncated=truncated,
            query_id=query_id,
            elapsed_ms=elapsed_ms,
        )
