"""Tests for RichTableOutput adapter."""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from openbb_cli.outputs.rich import RichTableOutput


@pytest.fixture()
def rich_output():
    return RichTableOutput()


@pytest.fixture()
def mock_session():
    with patch("openbb_cli.outputs.rich.session") as ms:
        ms.console.print = MagicMock()
        ms.settings.USE_INTERACTIVE_DF = False
        ms.backend = None
        yield ms


class TestRichOutputDisplay:
    """Tests for RichTableOutput.display()."""

    def test_export_true_no_output(self, rich_output, mock_session):
        with patch("openbb_cli.outputs.rich.print_rich_table") as mock_prt:
            rich_output.display(data=pd.DataFrame({"a": [1]}), export=True)
            mock_prt.assert_not_called()

    def test_obbject_with_list_results(self, rich_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {
            "results": [{"a": 1}, {"a": 2}]
        }
        with patch("openbb_cli.outputs.rich.print_rich_table") as mock_prt:
            rich_output.display(data=mock_obj)
            mock_prt.assert_called_once()
            df_arg = mock_prt.call_args[1]["df"]
            assert len(df_arg) == 2

    def test_obbject_with_none_results(self, rich_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": None}
        rich_output.display(data=mock_obj)
        mock_session.console.print.assert_called()
        msg = mock_session.console.print.call_args[0][0]
        assert "No results" in msg

    def test_obbject_with_empty_list(self, rich_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": []}
        rich_output.display(data=mock_obj)
        mock_session.console.print.assert_called()

    def test_obbject_with_dict_results(self, rich_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": {"x": 10}}
        with patch("openbb_cli.outputs.rich.print_rich_table") as mock_prt:
            rich_output.display(data=mock_obj)
            mock_prt.assert_called_once()

    def test_obbject_with_scalar_results(self, rich_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": 42}
        rich_output.display(data=mock_obj)
        # Scalar results are printed directly
        mock_session.console.print.assert_called()

    def test_dataframe(self, rich_output, mock_session):
        df = pd.DataFrame({"col": [1, 2, 3]})
        with patch("openbb_cli.outputs.rich.print_rich_table") as mock_prt:
            rich_output.display(data=df)
            mock_prt.assert_called_once()

    def test_empty_dataframe(self, rich_output, mock_session):
        df = pd.DataFrame()
        rich_output.display(data=df)
        mock_session.console.print.assert_called()

    def test_series(self, rich_output, mock_session):
        s = pd.Series([1, 2, 3], name="vals")
        with patch("openbb_cli.outputs.rich.print_rich_table") as mock_prt:
            rich_output.display(data=s)
            mock_prt.assert_called_once()

    def test_dict_input(self, rich_output, mock_session):
        with patch("openbb_cli.outputs.rich.print_rich_table") as mock_prt:
            rich_output.display(data={"a": [1, 2], "b": [3, 4]})
            mock_prt.assert_called_once()

    def test_list_input(self, rich_output, mock_session):
        with patch("openbb_cli.outputs.rich.print_rich_table") as mock_prt:
            rich_output.display(data=[{"a": 1}, {"a": 2}])
            mock_prt.assert_called_once()

    def test_scalar_input(self, rich_output, mock_session):
        rich_output.display(data=42)
        mock_session.console.print.assert_called_with(42)

    def test_chart_true_calls_show(self, rich_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": [{"a": 1}]}
        mock_obj.show.return_value = None
        rich_output.display(data=mock_obj, chart=True)
        mock_obj.show.assert_called_once()

    def test_chart_true_fallback_on_error(self, rich_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": [{"a": 1}]}
        mock_obj.show.side_effect = Exception("no chart")
        with patch("openbb_cli.outputs.rich.print_rich_table") as mock_prt:
            rich_output.display(data=mock_obj, chart=True)
            # Should fallback to table display
            mock_session.console.print.assert_called()

    def test_empty_list_input(self, rich_output, mock_session):
        rich_output.display(data=[])
        mock_session.console.print.assert_called()

    def test_interactive_mode_obbject(self, mock_session):
        """When USE_INTERACTIVE_DF is True and backend exists, try charting.table()."""
        mock_session.settings.USE_INTERACTIVE_DF = True
        mock_session.backend = MagicMock()

        rich_output = RichTableOutput()
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": [{"a": 1}]}
        mock_obj.charting.table.return_value = None

        rich_output.display(data=mock_obj)
        mock_obj.charting.table.assert_called_once()
