import pytest
from toolz import merge

from nmdc_runtime.site.graphs import ensure_jobs
from nmdc_runtime.site.repository import preset_normal


@pytest.skip("Needs supplied state")
def test_ensure_jobs():
    job = ensure_jobs.to_job(name="test_ensure_jobs", **preset_normal)
    run_config = merge({}, preset_normal["config"])
    run_config["ops"] = {
        "construct_jobs": {
            "config": {
                "base_jobs": [
                    {
                        "config": {"object_id": "gfs03r29"},
                        "workflow": {"id": "apply-changesheet-1.0"},
                    }
                ]
            }
        }
    }
    result = job.execute_in_process(run_config=run_config)

    assert result.success
