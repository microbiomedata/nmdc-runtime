from dagster import execute_solid
from nmdc_runtime.pipelines.core import mode_test
from nmdc_runtime.solids.core import hello


def test_hello():
    """
    This is an example test for a Dagster solid.

    For hints on how to test your Dagster solids, see our documentation tutorial on Testing:
    https://docs.dagster.io/tutorial/testable
    """
    result = execute_solid(hello, mode_def=mode_test)

    assert result.success
    assert result.output_value() == "Hello, NMDC!"
