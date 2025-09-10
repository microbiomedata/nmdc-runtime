import re

import pytest
from toolz import merge

from nmdc_runtime.api.core.metadata import load_changesheet
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.site.graphs import ensure_jobs
from nmdc_runtime.site.repository import preset_normal
from tests.test_api.test_endpoints import ensure_test_resources
from tests.test_api.test_metadata import TEST_DATA_DIR


@pytest.fixture
def can_apply_changesheet_via_job_config():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    with open(TEST_DATA_DIR.joinpath("changesheet-without-separator3.tsv")) as f:
        sheet_text = f.read()
    sheet_in = ChangesheetIn(
        name="sheet",
        content_type="text/tab-separated-values",
        text=sheet_text,
    )

    drs_obj_doc = persist_content_and_get_drs_object(
        content=sheet_in.text,
        username=rs["user"]["username"],
        filename=re.sub(r"[^A-Za-z0-9._\-]", "_", sheet_in.name),
        content_type=sheet_in.content_type,
        description="changesheet",
        id_ns="changesheets",
    )

    yield {
        "workflow": {"id": "apply-changesheet-1.0"},
        "config": {"object_id": drs_obj_doc["id"]},
    }

    mdb.objects.delete_one({"id": drs_obj_doc["id"]})


# Note: The `@pytest.mark.parametrize` decorator is used to set the so-called "fixture request parameter"
#       (accessed within the `seeded_db` fixture) to a particular value. The `indirect=True` flag tells pytest
#       to invoke the fixture having the specified name, and set the "fixture request parameter" to whatever
#       that fixture yields, rather than setting the "fixture request parameter" to this string, itself.
@pytest.mark.parametrize(
    "seeded_db", ["docs_for_seeded_db_for_changesheet_study_update"], indirect=True
)
def test_ensure_jobs(
    docs_for_seeded_db_for_changesheet_study_update,
    seeded_db,
    can_apply_changesheet_via_job_config,
):
    job = ensure_jobs.to_job(name="test_ensure_jobs", **preset_normal)
    run_config = merge({}, preset_normal["config"])
    run_config["ops"] = {
        "construct_jobs": {
            "config": {"base_jobs": [can_apply_changesheet_via_job_config]}
        }
    }
    result = job.execute_in_process(run_config=run_config)

    assert result.success
