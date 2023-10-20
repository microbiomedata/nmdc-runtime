import pytest
import requests
import requests_mock
from dagster import build_op_context

from nmdc_runtime.site.ops import get_csv_rows_from_url


def test_valid_data():
    context = build_op_context()
    with requests_mock.mock() as mock:
        mock.get(
            "http://www.example.com/data.csv",
            text='a,b,c\n1,hello,"apple, banana"\n2,wow,great',
        )

        result = get_csv_rows_from_url(context, "http://www.example.com/data.csv")
        assert result == [
            {"a": "1", "b": "hello", "c": "apple, banana"},
            {"a": "2", "b": "wow", "c": "great"},
        ]


def test_not_found():
    context = build_op_context()
    with requests_mock.mock() as mock:
        mock.get("http://www.example.com/data.csv", status_code=404)

        with pytest.raises(requests.HTTPError):
            get_csv_rows_from_url(context, "http://www.example.com/data.csv")
