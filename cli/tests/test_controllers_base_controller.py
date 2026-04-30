"""Test the base controller."""

import argparse
from unittest.mock import MagicMock, patch

import pytest
from openbb_cli.controllers.base_controller import BaseController

# pylint: disable=unused-argument, unused-variable


class DummyBaseController(BaseController):
    """Testable Base Controller."""

    def __init__(self, queue=None):
        """Initialize the TestableBaseController."""
        self.PATH = "/valid/path/"
        super().__init__(queue=queue)

    def print_help(self):
        """Print help."""


def test_base_controller_initialization():
    """Test the initialization of the base controller."""
    with patch.object(DummyBaseController, "check_path", return_value=None):
        controller = DummyBaseController()
        assert controller.path == ["valid", "path"]  # Checking for correct path split


def test_path_validation():
    """Test the path validation method."""
    controller = DummyBaseController()

    with pytest.raises(ValueError):
        controller.PATH = "invalid/path"
        controller.check_path()

    with pytest.raises(ValueError):
        controller.PATH = "/invalid/path"
        controller.check_path()

    with pytest.raises(ValueError):
        controller.PATH = "/Invalid/Path/"
        controller.check_path()

    controller.PATH = "/valid/path/"


def test_parse_input():
    """Test the parse input method."""
    controller = DummyBaseController()
    input_str = "cmd1/cmd2/cmd3"
    expected = ["cmd1", "cmd2", "cmd3"]
    result = controller.parse_input(input_str)
    assert result == expected


def test_switch():
    """Test the switch method."""
    controller = DummyBaseController()
    with patch.object(controller, "call_exit", MagicMock()) as mock_exit:
        controller.queue = ["exit"]
        controller.switch("exit")
        mock_exit.assert_called_once()


def test_call_help():
    """Test the call help method."""
    controller = DummyBaseController()
    with patch("openbb_cli.controllers.base_controller.session.console.print"):
        controller.call_help(None)


def test_call_exit():
    """Test the call exit method."""
    controller = DummyBaseController()
    with patch.object(controller, "save_class", MagicMock()):
        controller.queue = ["quit"]
        controller.call_exit(None)


@pytest.fixture
def mock_base_session():
    """Mock the session for parse_known_args_and_warn tests."""
    with patch("openbb_cli.controllers.base_controller.session") as mock_session:
        mock_session.settings.USE_CLEAR_AFTER_CMD = False
        yield mock_session


def _make_parser(*args_spec):
    """Create an argparse parser from a list of add_argument kwargs."""
    parser = argparse.ArgumentParser(add_help=False)
    for spec in args_spec:
        flags = spec.pop("flags")
        parser.add_argument(*flags, **spec)
    return parser


def test_comma_split_flagged_value_not_split(mock_base_session):
    """Simple test: --symbol AAPL,MSFT must stay as one value."""
    parser = _make_parser({"flags": ["--symbol", "-s"], "dest": "symbol", "type": str})
    result = BaseController.parse_known_args_and_warn(parser, ["--symbol", "AAPL,MSFT"])
    assert result is not None
    assert result.symbol == "AAPL,MSFT"


def test_comma_split_short_flag_not_split(mock_base_session):
    """Short flag -s AAPL,MSFT must also stay as one value."""
    parser = _make_parser({"flags": ["--symbol", "-s"], "dest": "symbol", "type": str})
    result = BaseController.parse_known_args_and_warn(parser, ["-s", "AAPL,MSFT"])
    assert result is not None
    assert result.symbol == "AAPL,MSFT"


def test_comma_split_equals_syntax_not_split(mock_base_session):
    """--symbol=AAPL,MSFT must not be split."""
    parser = _make_parser({"flags": ["--symbol", "-s"], "dest": "symbol", "type": str})
    result = BaseController.parse_known_args_and_warn(parser, ["--symbol=AAPL,MSFT"])
    assert result is not None
    assert result.symbol == "AAPL,MSFT"


def test_comma_split_nargs_plus_all_values_protected(mock_base_session):
    """nargs='+': all consecutive values after --symbols are protected."""
    parser = _make_parser(
        {"flags": ["--symbols"], "dest": "symbols", "nargs": "+", "type": str}
    )
    result = BaseController.parse_known_args_and_warn(
        parser, ["--symbols", "AAPL,MSFT", "GOOG,AMZN"]
    )
    assert result is not None
    assert result.symbols == ["AAPL,MSFT", "GOOG,AMZN"]


def test_comma_split_nargs_star_values_protected(mock_base_session):
    """nargs='*': consecutive values after --tags are protected."""
    parser = _make_parser(
        {"flags": ["--tags"], "dest": "tags", "nargs": "*", "type": str}
    )
    result = BaseController.parse_known_args_and_warn(parser, ["--tags", "a,b", "c,d"])
    assert result is not None
    assert result.tags == ["a,b", "c,d"]


def test_comma_split_nargs_int_values_protected(mock_base_session):
    """nargs=2: both values after --pair are protected."""
    parser = _make_parser(
        {"flags": ["--pair"], "dest": "pair", "nargs": 2, "type": str}
    )
    result = BaseController.parse_known_args_and_warn(parser, ["--pair", "a,b", "c,d"])
    assert result is not None
    assert result.pair == ["a,b", "c,d"]


def test_comma_split_store_true_not_confused(mock_base_session):
    """store_true flags (nargs=0) should not protect the next token."""
    parser = _make_parser(
        {"flags": ["--symbol", "-s"], "dest": "symbol", "type": str},
        {"flags": ["--raw"], "dest": "raw", "action": "store_true", "default": False},
    )
    result = BaseController.parse_known_args_and_warn(
        parser, ["--raw", "--symbol", "AAPL,MSFT"]
    )
    assert result is not None
    assert result.raw is True
    assert result.symbol == "AAPL,MSFT"


def test_comma_split_no_comma_values_unchanged(mock_base_session):
    """Values without commas pass through unaffected."""
    parser = _make_parser({"flags": ["--symbol", "-s"], "dest": "symbol", "type": str})
    result = BaseController.parse_known_args_and_warn(parser, ["--symbol", "AAPL"])
    assert result is not None
    assert result.symbol == "AAPL"


def test_comma_split_multiple_flags_each_protected(mock_base_session):
    """Multiple flags each protect their own values independently."""
    parser = _make_parser(
        {"flags": ["--symbol", "-s"], "dest": "symbol", "type": str},
        {"flags": ["--raw"], "dest": "raw", "action": "store_true", "default": False},
        {"flags": ["--provider"], "dest": "provider", "type": str},
    )
    result = BaseController.parse_known_args_and_warn(
        parser,
        ["--symbol", "AAPL,MSFT", "--provider", "yfinance,polygon"],
    )
    assert result is not None
    assert result.symbol == "AAPL,MSFT"
    assert result.provider == "yfinance,polygon"


# ── Tests for call_results ──────────────────────────────────────────


class TestCallResults:
    """Test the call_results method on BaseController."""

    def test_no_index_no_key_shows_all(self, mock_base_session):
        """Test results with no args shows all registry entries."""
        controller = DummyBaseController()
        mock_base_session.obbject_registry.all = {0: {"command": "test"}}
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index=None, key=None, chart=False, export="", sheet_name=None), [])
        )
        controller.call_results([])
        mock_base_session.output_adapter.display.assert_called_once()

    def test_no_results(self, mock_base_session):
        """Test results when registry is empty."""
        controller = DummyBaseController()
        mock_base_session.obbject_registry.all = {}
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index=None, key=None, chart=False, export="", sheet_name=None), [])
        )
        controller.call_results([])
        mock_base_session.console.print.assert_called()

    def test_by_index(self, mock_base_session):
        """Test results with --index flag."""
        controller = DummyBaseController()
        obbject = MagicMock()
        mock_base_session.obbject_registry.get.return_value = obbject
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index="0", key=None, chart=False, export="", sheet_name=None), [])
        )
        with patch("openbb_cli.controllers.base_controller.handle_obbject_display") as mock_display:
            controller.call_results(["-i", "0"])
            mock_display.assert_called_once()

    def test_by_invalid_index(self, mock_base_session):
        """Test results with non-integer index."""
        controller = DummyBaseController()
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index="abc", key=None, chart=False, export="", sheet_name=None), [])
        )
        controller.call_results(["-i", "abc"])
        mock_base_session.console.print.assert_called()

    def test_by_key(self, mock_base_session):
        """Test results with --key flag."""
        controller = DummyBaseController()
        obbject = MagicMock()
        mock_base_session.obbject_registry.get.return_value = obbject
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index=None, key="my_data", chart=False, export="", sheet_name=None), [])
        )
        with patch("openbb_cli.controllers.base_controller.handle_obbject_display") as mock_display:
            controller.call_results(["-k", "my_data"])
            mock_display.assert_called_once()

    def test_by_key_not_found(self, mock_base_session):
        """Test results with unknown key."""
        controller = DummyBaseController()
        mock_base_session.obbject_registry.get.return_value = None
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index=None, key="unknown", chart=False, export="", sheet_name=None), [])
        )
        controller.call_results(["-k", "unknown"])
        mock_base_session.console.print.assert_called()


# ── Tests for call_load ─────────────────────────────────────────────


class TestCallLoad:
    """Test the call_load method on BaseController."""

    def test_load_csv(self, mock_base_session, tmp_path):
        """Test loading a CSV file."""
        import pandas as pd

        csv_path = tmp_path / "test.csv"
        pd.DataFrame({"A": [1, 2], "B": [3, 4]}).to_csv(csv_path, index=False)

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)
        mock_base_session.obbject_registry.obbject_keys = []
        mock_base_session.obbject_registry.register.return_value = True
        mock_base_session.max_obbjects_exceeded = MagicMock(return_value=False)

        ns = MagicMock()
        ns.file = "test.csv"
        ns.sheet_name = None
        ns.register_key = ""
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "test.csv"])
        mock_base_session.obbject_registry.register.assert_called_once()

    def test_load_file_not_found(self, mock_base_session, tmp_path):
        """Test loading a nonexistent file."""
        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)

        ns = MagicMock()
        ns.file = "nonexistent.csv"
        ns.sheet_name = None
        ns.register_key = ""
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "nonexistent.csv"])
        mock_base_session.console.print.assert_called()
        call_args = mock_base_session.console.print.call_args[0][0]
        assert "not found" in call_args.lower() or "File not found" in call_args

    def test_load_json(self, mock_base_session, tmp_path):
        """Test loading a JSON file."""
        import pandas as pd

        json_path = tmp_path / "test.json"
        pd.DataFrame({"A": [1, 2], "B": [3, 4]}).to_json(json_path, orient="records")

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)
        mock_base_session.obbject_registry.obbject_keys = []
        mock_base_session.obbject_registry.register.return_value = True
        mock_base_session.max_obbjects_exceeded = MagicMock(return_value=False)

        ns = MagicMock()
        ns.file = "test.json"
        ns.sheet_name = None
        ns.register_key = ""
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "test.json"])
        mock_base_session.obbject_registry.register.assert_called_once()

    def test_load_excel(self, mock_base_session, tmp_path):
        """Test loading an Excel file."""
        import pandas as pd

        xlsx_path = tmp_path / "test.xlsx"
        pd.DataFrame({"A": [1, 2], "B": [3, 4]}).to_excel(xlsx_path, index=False)

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)
        mock_base_session.obbject_registry.obbject_keys = []
        mock_base_session.obbject_registry.register.return_value = True
        mock_base_session.max_obbjects_exceeded = MagicMock(return_value=False)

        ns = MagicMock()
        ns.file = "test.xlsx"
        ns.sheet_name = None
        ns.register_key = ""
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "test.xlsx"])
        mock_base_session.obbject_registry.register.assert_called_once()

    def test_load_sqlite(self, mock_base_session, tmp_path):
        """Test loading a SQLite database (lazy via SQLiteTable)."""
        import sqlite3

        import pandas as pd

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        pd.DataFrame({"A": [1, 2], "B": [3, 4]}).to_sql("prices", conn, index=False)
        conn.close()

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)
        mock_base_session.obbject_registry.obbject_keys = []
        mock_base_session.obbject_registry.register.return_value = True
        mock_base_session.max_obbjects_exceeded = MagicMock(return_value=False)

        ns = MagicMock()
        ns.file = "test.db"
        ns.sheet_name = None
        ns.register_key = ""
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "test.db"])
        mock_base_session.obbject_registry.register.assert_called_once()

    def test_load_register_key(self, mock_base_session, tmp_path):
        """Test loading with --register_key sets the key on the OBBject."""
        import pandas as pd

        csv_path = tmp_path / "keyed.csv"
        pd.DataFrame({"X": [10]}).to_csv(csv_path, index=False)

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)
        mock_base_session.obbject_registry.obbject_keys = []
        mock_base_session.obbject_registry.register.return_value = True
        mock_base_session.max_obbjects_exceeded = MagicMock(return_value=False)

        ns = MagicMock()
        ns.file = "keyed.csv"
        ns.sheet_name = None
        ns.register_key = "my_key"
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "keyed.csv", "--register_key", "my_key"])
        # Get the OBBject that was registered
        obbject = mock_base_session.obbject_registry.register.call_args[0][0]
        assert obbject.extra.get("register_key") == "my_key"

    def test_load_duplicate_register_key(self, mock_base_session, tmp_path):
        """Test loading with a duplicate register_key prints warning."""
        import pandas as pd

        csv_path = tmp_path / "dup.csv"
        pd.DataFrame({"X": [10]}).to_csv(csv_path, index=False)

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)
        mock_base_session.obbject_registry.obbject_keys = ["my_key"]
        mock_base_session.obbject_registry.register.return_value = True
        mock_base_session.max_obbjects_exceeded = MagicMock(return_value=False)

        ns = MagicMock()
        ns.file = "dup.csv"
        ns.sheet_name = None
        ns.register_key = "my_key"
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "dup.csv", "--register_key", "my_key"])
        console_calls = [str(c) for c in mock_base_session.console.print.call_args_list]
        assert any("already exists" in c for c in console_calls)

    def test_load_max_obbjects_exceeded(self, mock_base_session, tmp_path):
        """Test eviction when max obbjects is exceeded."""
        import pandas as pd

        csv_path = tmp_path / "evict.csv"
        pd.DataFrame({"X": [1]}).to_csv(csv_path, index=False)

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)
        mock_base_session.obbject_registry.obbject_keys = []
        mock_base_session.obbject_registry.register.return_value = True
        mock_base_session.max_obbjects_exceeded = MagicMock(return_value=True)

        ns = MagicMock()
        ns.file = "evict.csv"
        ns.sheet_name = None
        ns.register_key = ""
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "evict.csv"])
        mock_base_session.obbject_registry.remove.assert_called_once()
        console_calls = [str(c) for c in mock_base_session.console.print.call_args_list]
        assert any("oldest entry was removed" in c for c in console_calls)

    def test_load_register_failure(self, mock_base_session, tmp_path):
        """Test when register() returns False."""
        import pandas as pd

        csv_path = tmp_path / "fail_reg.csv"
        pd.DataFrame({"X": [1]}).to_csv(csv_path, index=False)

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)
        mock_base_session.obbject_registry.obbject_keys = []
        mock_base_session.obbject_registry.register.return_value = False
        mock_base_session.max_obbjects_exceeded = MagicMock(return_value=False)

        ns = MagicMock()
        ns.file = "fail_reg.csv"
        ns.sheet_name = None
        ns.register_key = ""
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "fail_reg.csv"])
        console_calls = [str(c) for c in mock_base_session.console.print.call_args_list]
        assert any("Failed to register" in c for c in console_calls)

    def test_load_unsupported_file_type(self, mock_base_session, tmp_path):
        """Test loading an unsupported file type."""
        bad_file = tmp_path / "data.parquet"
        bad_file.write_bytes(b"fake")

        controller = DummyBaseController()
        mock_base_session.user.preferences.data_directory = str(tmp_path)

        ns = MagicMock()
        ns.file = "data.parquet"
        ns.sheet_name = None
        ns.register_key = ""
        controller.parse_simple_args = MagicMock(return_value=(ns, []))
        with patch("openbb_cli.controllers.base_controller.session", mock_base_session):
            controller.call_load(["-f", "data.parquet"])
        console_calls = [str(c) for c in mock_base_session.console.print.call_args_list]
        assert any("Unsupported" in c for c in console_calls)


class TestCallResultsAdvanced:
    """Additional call_results tests for chart, export, and edge cases."""

    def test_by_index_with_chart(self, mock_base_session):
        """Test results with --index and --chart flag."""
        controller = DummyBaseController()
        obbject = MagicMock()
        mock_base_session.obbject_registry.get.return_value = obbject
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index="0", key=None, chart=True, export="", sheet_name=None), [])
        )
        with patch("openbb_cli.controllers.base_controller.handle_obbject_display") as mock_display:
            controller.call_results(["-i", "0", "--chart"])
            mock_display.assert_called_once()
            call_kwargs = mock_display.call_args[1]
            assert call_kwargs["chart"] is True

    def test_by_index_with_export(self, mock_base_session):
        """Test results with --index and --export flag."""
        controller = DummyBaseController()
        obbject = MagicMock()
        mock_base_session.obbject_registry.get.return_value = obbject
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index="0", key=None, chart=False, export="csv", sheet_name=None), [])
        )
        with patch("openbb_cli.controllers.base_controller.handle_obbject_display") as mock_display:
            controller.call_results(["-i", "0", "--export", "csv"])
            mock_display.assert_called_once()
            call_kwargs = mock_display.call_args[1]
            assert call_kwargs["export"] == "csv"

    def test_by_index_not_found(self, mock_base_session):
        """Test results with valid int index but no result."""
        controller = DummyBaseController()
        mock_base_session.obbject_registry.get.return_value = None
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index="5", key=None, chart=False, export="", sheet_name=None), [])
        )
        controller.call_results(["-i", "5"])
        mock_base_session.console.print.assert_called()
        call_args = mock_base_session.console.print.call_args[0][0]
        assert "No result" in call_args

    def test_by_key_with_chart_and_export(self, mock_base_session):
        """Test results with --key, --chart, and --export."""
        controller = DummyBaseController()
        obbject = MagicMock()
        mock_base_session.obbject_registry.get.return_value = obbject
        controller.parse_simple_args = MagicMock(
            return_value=(MagicMock(index=None, key="prices", chart=True, export="json", sheet_name="Sheet1"), [])
        )
        with patch("openbb_cli.controllers.base_controller.handle_obbject_display") as mock_display:
            controller.call_results(["-k", "prices", "--chart", "--export", "json"])
            call_kwargs = mock_display.call_args[1]
            assert call_kwargs["chart"] is True
            assert call_kwargs["export"] == "json"
            assert call_kwargs["sheet_name"] == "Sheet1"
