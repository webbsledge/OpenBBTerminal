"""Tests for JsonOutput adapter."""

import json
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from openbb_cli.outputs.json import JsonOutput


@pytest.fixture()
def json_output():
    return JsonOutput()


@pytest.fixture()
def mock_session():
    with patch("openbb_cli.outputs.json.session") as ms:
        ms.console.print = MagicMock()
        yield ms


class TestJsonOutputDisplay:
    """Tests for JsonOutput.display()."""

    def test_export_true_produces_no_output(self, json_output, mock_session):
        json_output.display(data={"key": "val"}, export=True)
        mock_session.console.print.assert_not_called()

    def test_obbject_with_list_results(self, json_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": [{"a": 1}, {"b": 2}]}
        json_output.display(data=mock_obj)
        printed = mock_session.console.print.call_args[0][0]
        parsed = json.loads(printed)
        assert parsed == [{"a": 1}, {"b": 2}]

    def test_obbject_with_dict_results(self, json_output, mock_session):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": {"x": 10}}
        json_output.display(data=mock_obj)
        parsed = json.loads(mock_session.console.print.call_args[0][0])
        assert parsed == {"x": 10}

    def test_dataframe(self, json_output, mock_session):
        df = pd.DataFrame({"col": [1, 2]})
        json_output.display(data=df)
        parsed = json.loads(mock_session.console.print.call_args[0][0])
        assert parsed == [{"col": 1}, {"col": 2}]

    def test_series(self, json_output, mock_session):
        s = pd.Series({"a": 1, "b": 2})
        json_output.display(data=s)
        parsed = json.loads(mock_session.console.print.call_args[0][0])
        assert parsed == {"a": 1, "b": 2}

    def test_scalar(self, json_output, mock_session):
        json_output.display(data=42)
        parsed = json.loads(mock_session.console.print.call_args[0][0])
        assert parsed == 42

    def test_chart_true_prints_warning(self, json_output, mock_session):
        json_output.display(data=42, chart=True)
        # chart warning is printed first, then the JSON data
        calls = mock_session.console.print.call_args_list
        warning_msg = calls[0][0][0]
        assert "not supported" in warning_msg.lower() or "JSON" in warning_msg

    def test_none_data(self, json_output, mock_session):
        json_output.display(data=None)
        printed = mock_session.console.print.call_args[0][0]
        assert json.loads(printed) is None
