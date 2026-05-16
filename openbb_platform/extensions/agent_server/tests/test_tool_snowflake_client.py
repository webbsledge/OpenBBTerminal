"""Snowflake client tests using a real sqlite3 connection."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pytest

from openbb_agent_server.plugins.tools.snowflake_tools.client import (
    SnowflakeClient,
    SnowflakeCredentials,
)
from openbb_agent_server.plugins.tools.snowflake_tools.safety import (
    SnowflakeSafetyViolation,
)


class _TestConn(sqlite3.Connection):
    """sqlite3 connection that adds the Snowflake-specific bits we use."""

    def cursor(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        cur = super().cursor(*args, **kwargs)
        return _TestCursor(cur)


class _TestCursor:
    """Wrap a sqlite3 cursor, adding ``sfqid`` and tolerating Snowflake-style placeholders."""

    def __init__(self, cur: sqlite3.Cursor) -> None:
        self._cur = cur
        self.sfqid = "synthetic-test-id"

    def execute(self, sql: str, params=None):  # type: ignore[no-untyped-def]
        if params is None:
            return self._cur.execute(sql)
        if isinstance(params, dict):
            # Translate Snowflake's %(name)s placeholders → sqlite's :name.
            converted = sql
            for key in params:
                converted = converted.replace(f"%({key})s", f":{key}")
            return self._cur.execute(converted, params)
        return self._cur.execute(sql, params)

    def fetchall(self):
        return list(self._cur.fetchall())

    def fetchmany(self, size):
        return list(self._cur.fetchmany(size))

    def fetchone(self):
        return self._cur.fetchone()

    @property
    def description(self):
        return self._cur.description

    def close(self) -> None:
        self._cur.close()


def _seed(conn: _TestConn) -> None:
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    for i in range(50):
        cur.execute("INSERT INTO t VALUES (?, ?)", (i, f"row-{i}"))
    conn.commit()


@pytest.fixture
def sqlite_factory() -> Iterator:
    conn = sqlite3.connect(":memory:", factory=_TestConn)
    _seed(conn)
    try:
        yield lambda creds: conn
    finally:
        conn.close()


def _client(factory, **overrides) -> SnowflakeClient:
    creds = SnowflakeCredentials(account="acc", user="u")
    return SnowflakeClient(creds, connection_factory=factory, **overrides)


def test_execute_select_returns_rows_and_query_id(sqlite_factory) -> None:
    client = _client(sqlite_factory, max_rows=10)
    result = client.execute("SELECT id, name FROM t")
    assert result.statement_kind == "SELECT"
    assert result.row_count == 10
    assert result.truncated is False  # 10 rows + LIMIT 10 fits exactly
    assert result.columns == ["id", "name"]
    assert result.query_id  # non-empty (synthetic when sqlite-backed)
    assert result.elapsed_ms is not None


def test_execute_show_tables(sqlite_factory) -> None:
    client = _client(sqlite_factory)
    # sqlite doesn't speak SHOW TABLES; substitute the equivalent.
    result = client.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert result.row_count >= 1


def test_execute_rejects_mutating_when_read_only(sqlite_factory) -> None:
    client = _client(sqlite_factory, read_only=True)
    with pytest.raises(SnowflakeSafetyViolation):
        client.execute("DROP TABLE t")


def test_execute_allows_mutating_when_read_only_disabled(sqlite_factory) -> None:
    client = _client(sqlite_factory, read_only=False)
    # Direct mutation succeeds (not normally exercised by an LLM, but
    # operators may toggle this off for known-safe pipelines).
    client.execute("UPDATE t SET name = 'updated' WHERE id = 0")


def test_max_rows_truncates_results(sqlite_factory) -> None:
    client = _client(sqlite_factory, max_rows=5)
    result = client.execute("SELECT id FROM t")
    assert result.row_count == 5


def test_per_call_max_rows_overrides_default(sqlite_factory) -> None:
    client = _client(sqlite_factory, max_rows=100)
    result = client.execute("SELECT id FROM t", max_rows=3)
    assert result.row_count == 3


def test_named_parameters_are_translated(sqlite_factory) -> None:
    client = _client(sqlite_factory)
    result = client.execute("SELECT id FROM t WHERE id = %(target)s", {"target": 7})
    assert result.rows == [[7]]


def test_open_close_round_trip(sqlite_factory) -> None:
    client = _client(sqlite_factory)
    client.open()
    client.close()
    # Re-open after close.
    client.open()
    client.close()


def test_credentials_to_connect_kwargs_excludes_empties() -> None:
    creds = SnowflakeCredentials(account="acc", user="u")
    kwargs = creds.to_connect_kwargs()
    assert kwargs["account"] == "acc"
    assert kwargs["user"] == "u"
    assert "password" not in kwargs
    assert "private_key" not in kwargs


def test_credentials_loads_pem_string() -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    creds = SnowflakeCredentials(account="acc", user="u", private_key=pem)
    kwargs = creds.to_connect_kwargs()
    assert isinstance(kwargs["private_key"], (bytes, bytearray))
    assert kwargs["authenticator"] == "snowflake"


def test_credentials_to_connect_kwargs_carries_schema_via_alias() -> None:
    creds = SnowflakeCredentials(account="acc", user="u", schema="S")
    kwargs = creds.to_connect_kwargs()
    assert kwargs["schema"] == "S"
