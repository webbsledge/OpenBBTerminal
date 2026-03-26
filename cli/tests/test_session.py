"""Test the Session class."""

from unittest.mock import MagicMock, patch

import pytest
from openbb_cli.models.settings import Settings
from openbb_cli.session import Session, sys

# pylint: disable=redefined-outer-name, unused-argument, protected-access


def mock_isatty(return_value):
    """Mock the isatty method."""
    original_isatty = sys.stdin.isatty
    sys.stdin.isatty = MagicMock(return_value=return_value)  # type: ignore
    return original_isatty


@pytest.fixture
def session():
    """Session fixture."""
    return Session()


def test_session_initialization(session):
    """Test the initialization of the Session class."""
    assert session.settings is not None
    assert session.style is not None
    assert session.console is not None
    assert session.obbject_registry is not None
    assert isinstance(session.settings, Settings)


@patch("openbb_cli.session.PromptSession")
@patch("sys.stdin.isatty", return_value=True)
def test_get_prompt_session_true(mock_isatty, mock_prompt_session, session):
    """Test get_prompt_session method."""
    prompt_session = session._get_prompt_session()
    assert prompt_session is not None


@patch("sys.stdin.isatty", return_value=False)
def test_get_prompt_session_false(mock_isatty, session):
    """Test get_prompt_session method."""
    prompt_session = session._get_prompt_session()
    assert prompt_session is None


# ── Tests for output_adapter ────────────────────────────────────────


def test_output_adapter_default(session):
    """Test output_adapter defaults to RichTableOutput."""
    adapter = session.output_adapter
    from openbb_cli.outputs.rich import RichTableOutput

    assert isinstance(adapter, RichTableOutput)


def test_output_adapter_cached(session):
    """Test output_adapter returns the same instance on subsequent calls."""
    adapter1 = session.output_adapter
    adapter2 = session.output_adapter
    assert adapter1 is adapter2


# ── Tests for max_obbjects_exceeded ─────────────────────────────────


def test_max_obbjects_not_exceeded(session):
    """Test max_obbjects_exceeded returns False when under limit."""
    session.settings.N_TO_KEEP_OBBJECT_REGISTRY = 10
    # Empty registry should not exceed
    assert session.max_obbjects_exceeded() is False


def test_max_obbjects_exceeded_at_limit():
    """Test max_obbjects_exceeded returns True when at limit."""
    s = Session()
    s.settings.N_TO_KEEP_OBBJECT_REGISTRY = 0
    # With 0 limit, any entries would exceed, but empty is at limit
    assert s.max_obbjects_exceeded() is True
