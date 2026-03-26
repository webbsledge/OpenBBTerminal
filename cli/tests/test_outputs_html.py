"""Tests for HtmlOutput adapter."""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from openbb_cli.outputs.html import HtmlOutput


@pytest.fixture()
def html_output():
    return HtmlOutput()


@pytest.fixture()
def mock_session():
    with patch("openbb_cli.outputs.html.session") as ms:
        ms.console.print = MagicMock()
        ms.settings.USE_INTERACTIVE_DF = False
        ms.backend = None
        yield ms


class TestHtmlOutputDisplay:
    """Tests for HtmlOutput.display()."""

    def test_export_true_no_output(self, html_output, mock_session):
        with patch("openbb_cli.outputs.html.webbrowser") as mock_wb:
            html_output.display(data=pd.DataFrame({"a": [1]}), export=True)
            mock_wb.open.assert_not_called()

    def test_obbject_with_list_results(self, html_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {
            "results": [{"a": 1}, {"a": 2}]
        }
        with patch("openbb_cli.outputs.html.webbrowser") as mock_wb:
            html_output.display(data=mock_obj)
            mock_wb.open.assert_called_once()
            # Verify the temp file path was passed
            url = mock_wb.open.call_args[0][0]
            assert url.startswith("file://") or ".html" in url or "tmp" in url.lower()

    def test_obbject_with_none_results(self, html_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": None}
        html_output.display(data=mock_obj)
        mock_session.console.print.assert_called()
        msg = mock_session.console.print.call_args[0][0]
        assert "No results" in msg

    def test_dataframe(self, html_output, mock_session):
        df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
        with patch("openbb_cli.outputs.html.webbrowser") as mock_wb:
            html_output.display(data=df)
            mock_wb.open.assert_called_once()

    def test_series(self, html_output, mock_session):
        s = pd.Series([10, 20], name="vals")
        with patch("openbb_cli.outputs.html.webbrowser") as mock_wb:
            html_output.display(data=s)
            mock_wb.open.assert_called_once()

    def test_dict_input(self, html_output, mock_session):
        with patch("openbb_cli.outputs.html.webbrowser") as mock_wb:
            html_output.display(data={"a": [1], "b": [2]})
            mock_wb.open.assert_called_once()

    def test_scalar_input(self, html_output, mock_session):
        with patch("openbb_cli.outputs.html.webbrowser") as mock_wb:
            html_output.display(data=42)
            mock_wb.open.assert_called_once()

    def test_chart_true_calls_show(self, html_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": [{"a": 1}]}
        mock_obj.show.return_value = None
        html_output.display(data=mock_obj, chart=True)
        mock_obj.show.assert_called_once()

    def test_chart_true_fallback_on_error(self, html_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {
            "results": [{"a": 1}]
        }
        mock_obj.show.side_effect = Exception("chart unavailable")
        with patch("openbb_cli.outputs.html.webbrowser") as mock_wb:
            html_output.display(data=mock_obj, chart=True)
            # Should fallback to HTML table
            mock_wb.open.assert_called_once()

    def test_interactive_mode_uses_charting_table(self, mock_session):
        """When interactive mode is on and backend exists, use charting.table()."""
        mock_session.settings.USE_INTERACTIVE_DF = True
        mock_session.backend = MagicMock()

        html_output = HtmlOutput()
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": [{"a": 1}]}
        mock_obj.charting.table.return_value = None

        html_output.display(data=mock_obj)
        mock_obj.charting.table.assert_called_once()

    def test_interactive_mode_fallback(self, mock_session):
        """If charting.table() fails, fall through to HTML rendering."""
        mock_session.settings.USE_INTERACTIVE_DF = True
        mock_session.backend = MagicMock()

        html_output = HtmlOutput()
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {
            "results": [{"a": 1}]
        }
        mock_obj.charting.table.side_effect = Exception("no PyWry")

        with patch("openbb_cli.outputs.html.webbrowser") as mock_wb:
            html_output.display(data=mock_obj)
            mock_wb.open.assert_called_once()
