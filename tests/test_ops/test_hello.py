from dagster import execute_solid

from nmdc_runtime.site.ops import hello


def test_hello():
    result = execute_solid(hello)

    assert result.success
    assert result.output_value() == "Hello, NMDC!"
