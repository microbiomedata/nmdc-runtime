from nmdc_runtime.site.graphs import hello_graph


def test_hello():
    """
    This is an example test for a Dagster pipeline.

    For hints on how to test your Dagster pipelines, see our documentation tutorial on Testing:
    https://docs.dagster.io/tutorial/testable
    """
    job = hello_graph.to_job()
    result = job.execute_in_process()

    assert result.success
    assert result.output_value("result") == "Hello, NMDC!"
