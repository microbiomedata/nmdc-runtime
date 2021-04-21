from dagster import execute_pipeline
from nmdc_runtime.pipelines.core import hello_pipeline


def test_hello_pipeline():
    """
    This is an example test for a Dagster pipeline.

    For hints on how to test your Dagster pipelines, see our documentation tutorial on Testing:
    https://docs.dagster.io/tutorial/testable
    """
    result = execute_pipeline(hello_pipeline, mode="test")

    assert result.success
    assert result.output_for_solid("hello") == "Hello, NMDC!"
