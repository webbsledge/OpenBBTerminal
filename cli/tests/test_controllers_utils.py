"""Test the Controller utils."""

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from openbb_cli.controllers.utils import (
    check_non_negative,
    check_positive,
    get_flair_and_username,
    get_user_agent,
    parse_and_split_input,
    print_goodbye,
    remove_file,
    welcome_message,
)

# pylint: disable=redefined-outer-name, unused-argument


@pytest.fixture
def mock_session():
    """Mock the session and its dependencies."""
    with patch("openbb_cli.controllers.utils.session") as mock_session:
        mock_session.console.print = MagicMock()
        mock_session.settings.VERSION = "1.0"
        mock_session.settings.FLAIR = "rocket"
        yield mock_session


def test_remove_file_existing_file():
    """Test removing an existing file."""
    with patch("os.path.isfile", return_value=True), patch("os.remove") as mock_remove:
        assert remove_file(Path("/path/to/file"))
        mock_remove.assert_called_once()


def test_remove_file_directory():
    """Test removing a directory."""
    with (
        patch("os.path.isfile", return_value=False),
        patch("os.path.isdir", return_value=True),
        patch("shutil.rmtree") as mock_rmtree,
    ):
        assert remove_file(Path("/path/to/directory"))
        mock_rmtree.assert_called_once()


def test_remove_file_failure(mock_session):
    """Test removing a file that fails."""
    with (
        patch("os.path.isfile", return_value=True),
        patch("os.remove", side_effect=Exception("Error")),
    ):
        assert not remove_file(Path("/path/to/file"))
        mock_session.console.print.assert_called()


def test_print_goodbye(mock_session):
    """Test printing the goodbye message."""
    print_goodbye()
    mock_session.console.print.assert_called()


def test_parse_and_split_input():
    """Test parsing and splitting user input."""
    user_input = "ls -f /home/user/docs/document.xlsx"
    result = parse_and_split_input(user_input, [])
    assert "ls" in result[0]


@pytest.mark.parametrize(
    "input_command, expected_output",
    [
        ("/", ["home"]),
        ("ls -f /path/to/file.txt", ["ls -f ", "path", "to", "file.txt"]),
        ("rm -f /home/user/docs", ["rm -f ", "home", "user", "docs"]),
    ],
)
def test_parse_and_split_input_special_cases(input_command, expected_output):
    """Test parsing and splitting user input with special cases."""
    result = parse_and_split_input(input_command, [])
    assert result == expected_output


def test_welcome_message(mock_session):
    """Test printing the welcome message."""
    welcome_message()
    mock_session.console.print.assert_called_with(
        "\nWelcome to OpenBB Platform CLI v1.0"
    )


def test_get_flair_and_username(mock_session):
    """Test getting the flair and username."""
    result = get_flair_and_username()
    assert "rocket" in result


@pytest.mark.parametrize(
    "value, expected",
    [
        ("10", 10),
        ("0", 0),
        ("-1", pytest.raises(argparse.ArgumentTypeError)),
        ("text", pytest.raises(ValueError)),
    ],
)
def test_check_non_negative(value, expected):
    """Test checking for a non-negative value."""
    if isinstance(expected, int):
        assert check_non_negative(value) == expected
    else:
        with expected:
            check_non_negative(value)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1", 1),
        ("0", pytest.raises(argparse.ArgumentTypeError)),
        ("-1", pytest.raises(argparse.ArgumentTypeError)),
        ("text", pytest.raises(ValueError)),
    ],
)
def test_check_positive(value, expected):
    """Test checking for a positive value."""
    if isinstance(expected, int):
        assert check_positive(value) == expected
    else:
        with expected:
            check_positive(value)


def test_get_user_agent():
    """Test getting the user agent."""
    result = get_user_agent()
    assert result.startswith("Mozilla/5.0")


# ── Tests for return_colored_value ──────────────────────────────────


from openbb_cli.controllers.utils import return_colored_value, suppress_stdout


@pytest.mark.parametrize(
    "value, expected_color",
    [
        ("10.5", "green"),
        ("-3.2", "red"),
        ("0", "yellow"),
        ("0.0", "yellow"),
        ("no numbers here", ""),
        ("abc 1 def 2", ""),  # two numbers → no color
    ],
)
def test_return_colored_value(value, expected_color):
    """Test return_colored_value applies correct color."""
    result = return_colored_value(value)
    if expected_color:
        assert f"[{expected_color}]" in result
    else:
        # No color wrapper when 0 or multiple numbers
        assert "[green]" not in result
        assert "[red]" not in result


# ── Tests for suppress_stdout ───────────────────────────────────────


import sys


def test_suppress_stdout():
    """Test that suppress_stdout redirects stdout and stderr."""
    original_out = sys.stdout
    original_err = sys.stderr
    with suppress_stdout():
        assert sys.stdout is not original_out
        assert sys.stderr is not original_err
    assert sys.stdout is original_out
    assert sys.stderr is original_err


# ── Tests for bootup ───────────────────────────────────────────────


from openbb_cli.controllers.utils import bootup


def test_bootup_runs_without_error(mock_session):
    """Test that bootup completes without raising."""
    bootup()


# ── Tests for first_time_user ──────────────────────────────────────


from openbb_cli.controllers.utils import first_time_user


def test_first_time_user_empty_env(mock_session, tmp_path):
    """Test first_time_user returns True for empty env file."""
    env_file = tmp_path / ".env"
    env_file.write_text("")
    with patch("openbb_cli.controllers.utils.ENV_FILE_SETTINGS", env_file):
        result = first_time_user()
    assert result is True
    mock_session.settings.set_item.assert_called_once_with("PREVIOUS_USE", True)


def test_first_time_user_non_empty_env(mock_session, tmp_path):
    """Test first_time_user returns False for non-empty env file."""
    env_file = tmp_path / ".env"
    env_file.write_text("KEY=VALUE\n")
    with patch("openbb_cli.controllers.utils.ENV_FILE_SETTINGS", env_file):
        result = first_time_user()
    assert result is False
