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


def test_resolved_checkpoint_path_default(tmp_path: Path) -> None:
    s = AgentServerSettings(data_dir=tmp_path)
    assert s.resolved_checkpoint_path() == str(tmp_path / "checkpoints.db")


def test_resolved_checkpoint_path_none_for_non_sqlite() -> None:
    s = AgentServerSettings(checkpointer_provider="inmemory")
    assert s.resolved_checkpoint_path() is None


def test_resolved_checkpoint_path_from_config(tmp_path: Path) -> None:
    s = AgentServerSettings(checkpointer_config={"path": str(tmp_path / "explicit.db")})
    assert s.resolved_checkpoint_path() == str(tmp_path / "explicit.db")


def test_resolved_checkpoint_path_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OPENBB_AGENT_CHECKPOINTER_PATH", str(tmp_path / "env.db"))
    s = AgentServerSettings()
    assert s.resolved_checkpoint_path() == str(tmp_path / "env.db")


def test_retention_config_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENBB_AGENT_PRUNE_INTERVAL_HOURS", raising=False)
    s = AgentServerSettings()
    assert s.checkpoint_keep_last == 1
    assert s.checkpoint_retention_days == 14
    assert s.history_retention_days == 90
    assert s.prune_interval_hours == 24
