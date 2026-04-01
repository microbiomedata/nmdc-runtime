from importlib.metadata import version

from nmdc_runtime.site.graphs import show_version_info_graph
from nmdc_runtime.site.repository import repo


def test_show_version_info():
    job = show_version_info_graph.to_job(name="show_version_info")
    result = job.execute_in_process()

    assert result.success
    assert result.output_value("result") == version("nmdc_runtime")


def test_show_version_info_is_in_repo():
    assert repo.get_job("show_version_info").name == "show_version_info"
