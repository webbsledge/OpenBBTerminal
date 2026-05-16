"""Settings load + URL resolution tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from openbb_agent_server.app.settings import AgentMetadata, AgentServerSettings


def test_default_settings_have_safe_loopback_host() -> None:
    s = AgentServerSettings()
    assert s.host == "127.0.0.1"
    assert s.port == 6900
    assert s.auth_backend == "none"


def test_resolved_db_url_falls_back_to_sqlite_under_data_dir(tmp_path: Path) -> None:
    s = AgentServerSettings(data_dir=tmp_path)
    url = s.resolved_db_url()
    assert url.startswith("sqlite+aiosqlite:///")
    assert str(tmp_path / "history.db") in url


def test_explicit_db_url_wins(tmp_path: Path) -> None:
    s = AgentServerSettings(
        data_dir=tmp_path,
        db_url="postgresql+psycopg://example:5432/db",
    )
    assert s.resolved_db_url() == "postgresql+psycopg://example:5432/db"


def test_settings_load_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENBB_AGENT_HOST", "0.0.0.0")
    monkeypatch.setenv("OPENBB_AGENT_PORT", "9999")
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "bearer_static")
    s = AgentServerSettings()
    assert s.host == "0.0.0.0"
    assert s.port == 9999
    assert s.auth_backend == "bearer_static"


def test_metadata_defaults_present() -> None:
    meta = AgentMetadata()
    assert meta.name
    assert meta.description
    assert meta.image_url is None
