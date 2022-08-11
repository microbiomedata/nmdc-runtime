from dagster import build_op_context

from nmdc_runtime.site.ops import hello


def test_hello():
    context = build_op_context()
    assert hello(context) == "Hello, NMDC!"
