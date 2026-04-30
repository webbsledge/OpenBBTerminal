"""Tests for TsvOutput adapter."""

from unittest.mock import Mock

import pandas as pd
import pytest

from openbb_cli.outputs.tsv import TsvOutput


@pytest.fixture()
def tsv_output():
    return TsvOutput()


class TestTsvOutputDisplay:
    """Tests for TsvOutput.display().

    The TSV adapter is a stub that processes data but does not actually print.
    Tests verify that display() completes without raising.
    """

    def test_export_true_no_output(self, tsv_output):
        result = tsv_output.display(data="anything", export=True)
        assert result is None

    def test_obbject_with_list_results(self, tsv_output):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {
            "results": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        }
        result = tsv_output.display(data=mock_obj)
        assert result is None

    def test_obbject_with_none_results(self, tsv_output):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": None}
        result = tsv_output.display(data=mock_obj)
        assert result is None

    def test_obbject_with_dict_results(self, tsv_output):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": {"x": 10}}
        result = tsv_output.display(data=mock_obj)
        assert result is None

    def test_obbject_with_scalar_results(self, tsv_output):
        mock_obj = Mock()
        mock_obj.model_dump.return_value = {"results": 42}
        result = tsv_output.display(data=mock_obj)
        assert result is None

    def test_dataframe(self, tsv_output):
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = tsv_output.display(data=df)
        assert result is None

    def test_series(self, tsv_output):
        s = pd.Series([10, 20], name="vals")
        result = tsv_output.display(data=s)
        assert result is None

    def test_dict_input(self, tsv_output):
        result = tsv_output.display(data={"a": [1], "b": [2]})
        assert result is None

    def test_list_input(self, tsv_output):
        result = tsv_output.display(data=[{"val": 5}])
        assert result is None

    def test_scalar_input(self, tsv_output):
        result = tsv_output.display(data="hello")
        assert result is None

    def test_chart_with_chart_attr(self, tsv_output):
        mock_obj = Mock()
        mock_obj.chart = "chart-object-repr"
        # TSV adapter returns early when chart=True and data has chart attr
        result = tsv_output.display(data=mock_obj, chart=True)
        assert result is None

    def test_export_guard(self, tsv_output):
        df = pd.DataFrame({"a": [1, 2], "b": [None, None]})
        result = tsv_output.display(data=df)
        assert result is None
