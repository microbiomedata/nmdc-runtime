import json
import os
import re
import subprocess
import sys

import bson
import pytest
import requests
from dagster import build_op_context
from starlette import status
from tenacity import wait_random_exponential, retry
from toolz import get_in

from nmdc_runtime.api.core.auth import get_password_hash
from nmdc_runtime.api.core.metadata import df_from_sheet_in, _validate_changesheet
from nmdc_runtime.api.core.util import generate_secret, dotted_path_for
from nmdc_runtime.api.db.mongo import get_mongo_db, mongorestore_from_dir
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.job import Job, JobOperationMetadata
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.site import SiteInDB, SiteClientInDB
from nmdc_runtime.api.models.user import UserInDB, UserIn, User
from nmdc_runtime.site.ops import materialize_alldocs
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo, RuntimeApiSiteClient, mongo_resource
from nmdc_runtime.util import REPO_ROOT_DIR, ensure_unique_id_indexes
from tests.test_util import download_and_extract_tar
from tests.test_ops.test_ops import op_context as test_op_context

TEST_MONGODUMPS_DIR = REPO_ROOT_DIR.joinpath("tests", "nmdcdb")
SCHEMA_COLLECTIONS_MONGODUMP_ARCHIVE_BASENAME = (
    "nmdc-prod-schema-collections__2024-07-29_20-12-07"
)
SCHEMA_COLLECTIONS_MONGODUMP_ARCHIVE_URL = (
    "https://portal.nersc.gov/cfs/m3408/meta/mongodumps/"
    f"{SCHEMA_COLLECTIONS_MONGODUMP_ARCHIVE_BASENAME}.tar"
)  # 84MB. Should be < 100MB.


def ensure_local_mongodump_exists():
    dump_dir = TEST_MONGODUMPS_DIR.joinpath(
        SCHEMA_COLLECTIONS_MONGODUMP_ARCHIVE_BASENAME
    )
    if not os.path.exists(dump_dir):
        download_and_extract_tar(
            url=SCHEMA_COLLECTIONS_MONGODUMP_ARCHIVE_URL, extract_to=TEST_MONGODUMPS_DIR
        )
    else:
        print(f"local mongodump already exists at {TEST_MONGODUMPS_DIR}")
    return dump_dir


def ensure_schema_collections_and_alldocs():
    # Return if `alldocs` collection has already been materialized.
    mdb = get_mongo_db()
    if mdb.alldocs.estimated_document_count() > 0:
        print(
            "ensure_schema_collections_and_alldocs: `alldocs` collection already materialized"
        )
        return

    dump_dir = ensure_local_mongodump_exists()
    mongorestore_from_dir(mdb, dump_dir, skip_collections=["functional_annotation_agg"])
    ensure_unique_id_indexes(mdb)
    print("materializing alldocs...")
    materialize_alldocs(
        build_op_context(
            resources={
                "mongo": mongo_resource.configured(
                    {
                        "dbname": os.getenv("MONGO_DBNAME"),
                        "host": os.getenv("MONGO_HOST"),
                        "password": os.getenv("MONGO_PASSWORD"),
                        "username": os.getenv("MONGO_USERNAME"),
                    }
                )
            }
        )
    )


def ensure_test_resources(mdb):
    username = "testuser"
    password = generate_secret()
    site_id = "testsite"
    mdb.users.replace_one(
        {"username": username},
        UserInDB(
            username=username,
            hashed_password=get_password_hash(password),
            site_admin=[site_id],
        ).model_dump(exclude_unset=True),
        upsert=True,
    )

    client_id = "testsite-testclient"
    client_secret = generate_secret()
    mdb.sites.replace_one(
        {"id": site_id},
        SiteInDB(
            id=site_id,
            clients=[
                SiteClientInDB(
                    id=client_id,
                    hashed_secret=get_password_hash(client_secret),
                )
            ],
        ).model_dump(),
        upsert=True,
    )
    wf_id = "test"
    job_id = "nmdc:fk0jb83"
    prev_ops = {"metadata.job.id": job_id, "metadata.site_id": site_id}
    mdb.operations.delete_many(prev_ops)
    job = Job(**{"id": job_id, "workflow": {"id": wf_id}, "config": {}, "claims": []})
    mdb.jobs.replace_one(
        {"id": job_id}, job.model_dump(exclude_unset=True), upsert=True
    )
    mdb["minter.requesters"].replace_one({"id": site_id}, {"id": site_id}, upsert=True)
    ensure_schema_collections_and_alldocs()
    return {
        "site_client": {
            "site_id": site_id,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        "user": {"username": username, "password": password},
        "job": job.model_dump(exclude_unset=True),
    }


@pytest.mark.skip(reason="Skipping because test causes suite to hang")
def test_update_operation():
    mdb = get_mongo(run_config_frozen__normal_env).db
    rs = ensure_test_resources(mdb)
    client = RuntimeApiSiteClient(base_url=os.getenv("API_HOST"), **rs["site_client"])
    rv = client.claim_job(rs["job"]["id"])
    assert "id" in rv.json()
    op = rv.json()
    new_op = client.update_operation(op["id"], {"metadata": {"foo": "bar"}}).json()
    assert get_in(["metadata", "site_id"], new_op) == rs["site_client"]["site_id"]
    assert get_in(["metadata", "job", "id"], new_op) == rs["job"]["id"]
    assert get_in(["metadata", "model"], new_op) == dotted_path_for(
        JobOperationMetadata
    )


@pytest.mark.skip(reason="Skipping because test causes suite to hang")
def test_create_user():
    mdb = get_mongo(run_config_frozen__normal_env).db
    rs = ensure_test_resources(mdb)
    base_url = os.getenv("API_HOST")

    @retry(wait=wait_random_exponential(multiplier=1, max=60))
    def get_token():
        """

        Randomly wait up to 2^x * 1 seconds between each retry until the range reaches 60
        seconds, then randomly up to 60 seconds afterwards

        """

        _rv = requests.post(
            base_url + "/token",
            data={
                "grant_type": "password",
                "username": rs["user"]["username"],
                "password": rs["user"]["password"],
            },
        )
        token_response = _rv.json()
        return token_response["access_token"]

    headers = {"Authorization": f"Bearer {get_token()}"}

    user_in = UserIn(username="foo", password=generate_secret())
    mdb.users.delete_one({"username": user_in.username})
    mdb.users.update_one(
        {"username": rs["user"]["username"]},
        {"$addToSet": {"site_admin": "nmdc-runtime-useradmin"}},
    )
    rv = requests.request(
        "POST",
        url=(base_url + "/users"),
        headers=headers,
        json=user_in.model_dump(exclude_unset=True),
    )

    try:
        assert rv.status_code == status.HTTP_201_CREATED
        User(**rv.json())
    finally:
        mdb.users.delete_one({"username": user_in.username})
        mdb.users.update_one(
            {"username": rs["user"]["username"]},
            {"$pull": {"site_admin": "nmdc-runtime-useradmin"}},
        )


@pytest.fixture
def api_site_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiSiteClient(base_url=os.getenv("API_HOST"), **rs["site_client"])


def test_metadata_validate_json_0(api_site_client):
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {
            "field_research_site_set": [
                {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
                {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
                {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
            ]
        },
    )
    assert rv.json()["result"] == "errors"


def test_metadata_validate_json_empty_collection(api_site_client):
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {"study_set": []},
    )
    assert rv.json()["result"] != "errors"


def test_metadata_validate_json_with_type_attribute(api_site_client):
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {"study_set": [], "@type": "Database"},
    )
    assert rv.json()["result"] != "errors"
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {"study_set": [], "@type": "nmdc:Database"},
    )
    assert rv.json()["result"] != "errors"


def test_metadata_validate_json_with_unknown_collection(api_site_client):
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {"studi_set": []},
    )
    assert rv.json()["result"] == "errors"


def test_submit_changesheet():
    sheet_in = ChangesheetIn(
        name="sheet",
        content_type="text/tab-separated-values",
        text="id\taction\tattribute\tvalue\nnmdc:bsm-12-7mysck21\tupdate\tpart_of\tnmdc:sty-11-pzmd0x14\n",
    )
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    if not mdb.biosample_set.find_one({"id": "nmdc:bsm-12-7mysck21"}):
        mdb.biosample_set.insert_one(
            json.loads(
                (
                    REPO_ROOT_DIR / "tests" / "files" / "nmdc_bsm-12-7mysck21.json"
                ).read_text()
            )
        )
    if not mdb.study_set.find_one({"id": "nmdc:sty-11-pzmd0x14"}):
        mdb.study_set.insert_one(
            json.loads(
                (
                    REPO_ROOT_DIR / "tests" / "files" / "nmdc_sty-11-pzmd0x14.json"
                ).read_text()
            )
        )
    df_change = df_from_sheet_in(sheet_in, mdb)
    _ = _validate_changesheet(df_change, mdb)

    # clear objects collection to avoid strange duplicate key error.
    mdb.objects.delete_many({})

    drs_obj_doc = persist_content_and_get_drs_object(
        content=sheet_in.text,
        username=rs["user"]["username"],
        filename=re.sub(r"[^A-Za-z0-9._\-]", "_", sheet_in.name),
        content_type=sheet_in.content_type,
        description="changesheet",
        id_ns="changesheets",
    )
    mdb.objects.delete_one({"id": drs_obj_doc["id"]})
    assert True


@pytest.mark.skip(
    reason="Skipping because race condition causes  http://fastapi:8000/nmdcschema/ids/nmdc:wfrqc-11-t0tvnp52.2 to 404?"
)
def test_submit_workflow_activities(api_site_client):
    test_collection, test_id = (
        "read_qc_analysis_activity_set",
        "nmdc:wfrqc-11-t0tvnp52.2",
    )
    test_payload = {
        test_collection: [
            {
                "id": test_id,
                "name": "Read QC Activity for nmdc:wfrqc-11-t0tvnp52.1",
                "started_at_time": "2024-01-11T20:48:30.718133+00:00",
                "ended_at_time": "2024-01-11T21:11:44.884260+00:00",
                "was_informed_by": "nmdc:omprc-11-9mvz7z22",
                "execution_resource": "NERSC-Perlmutter",
                "git_url": "https://github.com/microbiomedata/ReadsQC",
                "has_input": ["nmdc:dobj-11-gpthnj64"],
                "has_output": [
                    "nmdc:dobj-11-w5dak635",
                    "nmdc:dobj-11-g6d71n77",
                    "nmdc:dobj-11-bds7qq03",
                ],
                "type": "nmdc:ReadQcAnalysisActivity",
                "part_of": ["nmdc:omprc-11-9mvz7z22"],
                "version": "v1.0.8",
            }
        ]
    }
    mdb = get_mongo_db()
    if doc_to_restore := mdb[test_collection].find_one({"id": test_id}):
        mdb[test_collection].delete_one({"id": test_id})
    rv = api_site_client.request(
        "POST",
        "/v1/workflows/activities",
        test_payload,
    )
    assert rv.json() == {"message": "jobs accepted"}
    rv = api_site_client.request("GET", f"/nmdcschema/ids/{test_id}")
    mdb[test_collection].delete_one({"id": test_id})
    if doc_to_restore:
        mdb[test_collection].insert_one(doc_to_restore)
    assert "id" in rv.json() and "input_read_count" not in rv.json()


def test_get_class_name_and_collection_names_by_doc_id():
    base_url = os.getenv("API_HOST")

    # Seed the database.
    mdb = get_mongo_db()
    study_set_collection = mdb.get_collection(name="study_set")
    study_set_collection.insert_one(dict(id="nmdc:sty-1-foobar"))

    # Valid `id`, and the document exists in database.
    id_ = "nmdc:sty-1-foobar"
    response = requests.request(
        "GET", f"{base_url}/nmdcschema/ids/{id_}/collection-name"
    )
    body = response.json()
    assert response.status_code == 200
    assert body["id"] == id_
    assert body["collection_name"] == "study_set"

    # Valid `id`, but the document does not exist in database.
    id_ = "nmdc:sty-1-bazqux"
    response = requests.request(
        "GET", f"{base_url}/nmdcschema/ids/{id_}/collection-name"
    )
    assert response.status_code == 404

    # Invalid `id` (because "foo" is an invalid typecode).
    id_ = "nmdc:foo-1-foobar"
    response = requests.request(
        "GET", f"{base_url}/nmdcschema/ids/{id_}/collection-name"
    )
    assert response.status_code == 404


def test_find_data_objects_for_study(api_site_client):
    ensure_schema_collections_and_alldocs()
    rv = api_site_client.request(
        "GET",
        "/data_objects/study/nmdc:sty-11-hdd4bf83",
    )
    assert len(rv.json()) >= 60
