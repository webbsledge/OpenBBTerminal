"""Tests for StdioOutput adapter."""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from openbb_cli.outputs.stdio import StdioOutput


@pytest.fixture()
def stdio_output():
    return StdioOutput()


@pytest.fixture()
def mock_session():
    with patch("openbb_cli.outputs.stdio.session") as ms:
        ms.console.print = MagicMock()
        yield ms


class TestStdioOutputDisplay:
    """Tests for StdioOutput.display().

    The STDIO adapter is a stub that processes data but does not call
    builtins.print. It only uses session.console.print for the chart warning.
    Tests verify display() completes without raising.
    """

    def test_export_true_no_output(self, stdio_output, mock_session):
        result = stdio_output.display(data="anything", export=True)
        assert result is None

    def test_obbject_with_list_results(self, stdio_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {
            "results": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        }
        result = stdio_output.display(data=mock_obj)
        assert result is None

    def test_obbject_with_none_results(self, stdio_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": None}
        result = stdio_output.display(data=mock_obj)
        assert result is None

    def test_obbject_with_scalar_results(self, stdio_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": 42}
        result = stdio_output.display(data=mock_obj)
        assert result is None

    def test_dataframe(self, stdio_output, mock_session):
        df = pd.DataFrame({"x": [10, 20], "y": [30, 40]})
        result = stdio_output.display(data=df)
        assert result is None

    def test_series(self, stdio_output, mock_session):
        s = pd.Series([5, 6], name="vals")
        result = stdio_output.display(data=s)
        assert result is None

    def test_dict_input(self, stdio_output, mock_session):
        result = stdio_output.display(data={"a": [1], "b": [2]})
        assert result is None

    def test_list_input(self, stdio_output, mock_session):
        result = stdio_output.display(data=[{"val": 5}])
        assert result is None

    def test_scalar_input(self, stdio_output, mock_session):
        result = stdio_output.display(data="hello")
        assert result is None

    def test_chart_true_prints_warning(self, stdio_output, mock_session):
        stdio_output.display(data=42, chart=True)
        mock_session.console.print.assert_called()
