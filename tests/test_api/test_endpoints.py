import json
import os
import re
from typing import List

import pytest
import requests
from dagster import build_op_context
from starlette import status
from tenacity import wait_random_exponential, stop_after_attempt, retry
from toolz import get_in

from nmdc_runtime.api.core.auth import get_password_hash
from nmdc_runtime.api.core.metadata import df_from_sheet_in, _validate_changesheet
from nmdc_runtime.api.core.util import generate_secret, dotted_path_for
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
    get_collection_names_from_schema,
    mongorestore_collection,
)
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.job import Job, JobOperationMetadata
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.site import SiteInDB, SiteClientInDB
from nmdc_runtime.api.models.user import UserInDB, UserIn, User
from nmdc_runtime.site.ops import materialize_alldocs
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import (
    get_mongo,
    RuntimeApiSiteClient,
    mongo_resource,
    RuntimeApiUserClient,
)
from nmdc_runtime.util import REPO_ROOT_DIR, ensure_unique_id_indexes, validate_json
from tests.test_util import download_to
from tests.lib.faker import Faker


# Instantiate a faker that we can use to generate fake data for testing.
faker = Faker()

# TODO: Why is there a 43 MB `tests/nmdcdb.test.archive.gz` file in the repository
#       at the same time as this dump-downloading code? If the aforementioned file
#       is obsolete, delete it.
#
MONGODUMP_URL_PREFIX = os.getenv("MONGO_REMOTE_DUMP_URL_PREFIX")
# extract the specific dump directory name, e.g. 'dump_nmdc-prod_2025-02-12_20-12-02'.
MONGODUMP_DIRNAME = MONGODUMP_URL_PREFIX.rsplit("/", maxsplit=1)[-1]
MONGODUMP_DIR = REPO_ROOT_DIR / "tests" / "nmdcdb" / MONGODUMP_DIRNAME
MONGODUMP_DIR.mkdir(parents=True, exist_ok=True)

# certain schema collections are not expected for tests
SCHEMA_COLLECTIONS_EXCLUDED = {
    "functional_annotation_agg",
    "functional_annotation_set",
    "genome_feature_set",
}


def ensure_schema_collections_local_filesystem_cache():
    r"""
    Downloads the dumps of MongoDB collections that are not already present in a local directory.
    
    Parameters: 
    - It downloads dumps from the remote host specified via `MONGODUMP_URL_PREFIX`.
    - It downloads the dumps into the local directory specified via `MONGODUMP_DIR`.
    - It does not download dumps of MongoDB collections whose names are in `SCHEMA_COLLECTIONS_EXCLUDED`.
    """

    # download collections into "nmdc" db namespace
    db_dir = MONGODUMP_DIR / "nmdc"
    db_dir.mkdir(exist_ok=True)
    for name in get_collection_names_from_schema():
        if name in SCHEMA_COLLECTIONS_EXCLUDED:
            continue
        url = f"{MONGODUMP_URL_PREFIX}/nmdc/{name}.bson.gz"
        target_path = db_dir / (name + ".bson.gz")
        # TODO use sha256 hashes or at least file sizes to ensure fidelity of existing files.
        #   Can use HEAD request on `url`?
        if not db_dir.joinpath(name + ".bson.gz").exists():
            download_to(url, target_path)


def ensure_schema_collections_loaded():
    r"""
    Downloads any missing MongoDB collection dumps from a remote host into a local directory,
    then restores any non-empty dumps in that directory into the MongoDB database.

    Parameters:
    - It looks for the dumps in the local directory specified via `MONGODUMP_DIR`.
    - It does not restore dumps of MongoDB collections whose names are in `SCHEMA_COLLECTIONS_EXCLUDED`.
    """

    ensure_schema_collections_local_filesystem_cache()
    mdb = get_mongo_db()
    for name in get_collection_names_from_schema():
        if name in SCHEMA_COLLECTIONS_EXCLUDED:
            continue
        if not mdb.get_collection(name).estimated_document_count() > 0:
            filepath = MONGODUMP_DIR / "nmdc" / (name + ".bson.gz")
            print(f"ensure_schema_collections_loaded: restoring {name}")
            mongorestore_collection(mdb, name, filepath)


def ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs: bool = False):
    r"""
    This function can be used to ensure the "alldocs" collection has been materialized.

    :param bool force_refresh_of_alldocs: Whether you want to force a refresh of the "alldocs" collection,
                                          regardless of whether it is empty or not. By default, this function
                                          will only refresh the "alldocs" collection if it is empty.
    """
    mdb = get_mongo_db()
    ensure_unique_id_indexes(mdb)
    # Return if `alldocs` collection has already been materialized, and caller does not want to force a refresh of it.
    if mdb.alldocs.estimated_document_count() > 0 and not force_refresh_of_alldocs:
        print(
            "ensure_alldocs_collection_has_been_materialized: `alldocs` collection already materialized"
        )
        return

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
    ensure_alldocs_collection_has_been_materialized()
    return {
        "site_client": {
            "site_id": site_id,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        "user": {"username": username, "password": password},
        "job": job.model_dump(exclude_unset=True),
    }


@pytest.fixture
def api_site_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiSiteClient(base_url=os.getenv("API_HOST"), **rs["site_client"])


@pytest.fixture
def api_user_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiUserClient(base_url=os.getenv("API_HOST"), **rs["user"])


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


def test_create_user():
    mdb = get_mongo(run_config_frozen__normal_env).db
    rs = ensure_test_resources(mdb)
    base_url = os.getenv("API_HOST")

    @retry(
        wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(3)
    )
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


def test_update_user():
    mdb = get_mongo(run_config_frozen__normal_env).db
    rs = ensure_test_resources(mdb)
    base_url = os.getenv("API_HOST")

    # Try up to three times, waiting for up to 60 seconds between each attempt.
    @retry(
        wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(3)
    )
    def get_token():
        """
        Fetch an auth token from the Runtime API.
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

    user_in1 = UserIn(username="foo", password="oldpass")
    mdb.users.delete_one({"username": user_in1.username})
    mdb.users.update_one(
        {"username": rs["user"]["username"]},
        {"$addToSet": {"site_admin": "nmdc-runtime-useradmin"}},
    )
    rv_create = requests.request(
        "POST",
        url=(base_url + "/users"),
        headers=headers,
        json=user_in1.model_dump(exclude_unset=True),
    )

    u1 = mdb.users.find_one({"username": user_in1.username})

    user_in2 = UserIn(username="foo", password="newpass")

    rv_update = requests.request(
        "PUT",
        url=(base_url + "/users"),
        headers=headers,
        json=user_in2.model_dump(exclude_unset=True),
    )

    u2 = mdb.users.find_one({"username": user_in2.username})
    try:
        assert rv_create.status_code == status.HTTP_201_CREATED
        assert rv_update.status_code == status.HTTP_200_OK
        assert u1["hashed_password"] != u2["hashed_password"]

    finally:
        mdb.users.delete_one({"username": user_in1.username})
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
        text="id\taction\tattribute\tvalue\nnmdc:bsm-12-7mysck21\tupdate\tassociated_studies\tnmdc:sty-11-pzmd0x14\n",
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


def test_submit_workflow_activities(api_site_client):
    test_collection, test_id = (
        "workflow_execution_set",
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
                "type": "nmdc:ReadQcAnalysis",
                "version": "v1.0.8",
            }
        ]
    }
    mdb = get_mongo_db()
    if doc_to_restore := mdb[test_collection].find_one({"id": test_id}):
        mdb[test_collection].delete_one({"id": test_id})
    rv = api_site_client.request(
        "POST",
        "/workflows/workflow_executions",
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
    my_study = {"id": "nmdc:sty-1-foobar", "type": "nmdc:Study"}
    study_set_collection.replace_one(my_study, my_study, upsert=True)

    # Valid `id`, and the document exists in database.
    id_ = my_study["id"]
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


def test_find_data_objects_for_nonexistent_study(api_site_client):
    r"""
    Confirms the endpoint returns an unsuccessful status code when no `Study` having the specified `id` exists.
    Reference: https://docs.pytest.org/en/stable/reference/reference.html#pytest.raises

    Note: The `api_site_client` fixture's `request` method will raise an exception if the server responds with
          an unsuccessful status code.
    """
    ensure_alldocs_collection_has_been_materialized()
    with pytest.raises(requests.exceptions.HTTPError):
        api_site_client.request(
            "GET",
            "/data_objects/study/nmdc:sty-11-fake",
        )


def test_find_data_objects_for_study_having_none(api_site_client):
    # Seed the test database with a study having no associated data objects.
    mdb = get_mongo_db()
    study_id = "nmdc:sty-00-beeeeeef"
    study_dict = {
        "id": study_id,
        "type": "nmdc:Study",
        "study_category": "research_study",
    }
    assert validate_json({"study_set": [study_dict]}, mdb)["result"] != "errors"

    mdb.get_collection(name="study_set").replace_one(
        {"id": study_id}, study_dict, upsert=True
    )

    # Update the `alldocs` collection, which is a cache used by the endpoint under test.
    ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    # Confirm the endpoint responds with no data objects.
    response = api_site_client.request("GET", f"/data_objects/study/{study_id}")
    assert response.status_code == 200
    data_objects_by_biosample = response.json()
    assert len(data_objects_by_biosample) == 0

    # Clean up: Delete the documents we created within this test, from the database.
    mdb.get_collection(name="study_set").delete_one({"id": study_id})
    mdb.get_collection(name="alldocs").delete_many({})


def test_find_data_objects_for_study_having_one(api_site_client):
    # Seed the test database with a study having one associated data object.
    mdb = get_mongo_db()
    study_id = "nmdc:sty-11-r2h77870"
    study_dict = {
        "id": study_id,
        "type": "nmdc:Study",
        "study_category": "research_study",
    }
    fakes = set()
    assert validate_json({"study_set": [study_dict]}, mdb)["result"] != "errors"
    if mdb.get_collection(name="study_set").find_one({"id": study_id}) is None:
        mdb.get_collection(name="study_set").insert_one(study_dict)
        fakes.add("study")
    biosample_id = "nmdc:bsm-11-6zd5nb38"
    biosample_dict = {
        "id": biosample_id,
        "env_broad_scale": {
            "has_raw_value": "ENVO_00000446",
            "term": {
                "id": "ENVO:00000446",
                "name": "terrestrial biome",
                "type": "nmdc:OntologyClass",
            },
            "type": "nmdc:ControlledIdentifiedTermValue",
        },
        "env_local_scale": {
            "has_raw_value": "ENVO_00005801",
            "term": {
                "id": "ENVO:00005801",
                "name": "rhizosphere",
                "type": "nmdc:OntologyClass",
            },
            "type": "nmdc:ControlledIdentifiedTermValue",
        },
        "env_medium": {
            "has_raw_value": "ENVO_00001998",
            "term": {
                "id": "ENVO:00001998",
                "name": "soil",
                "type": "nmdc:OntologyClass",
            },
            "type": "nmdc:ControlledIdentifiedTermValue",
        },
        "type": "nmdc:Biosample",
        "associated_studies": [study_id],
    }
    assert validate_json({"biosample_set": [biosample_dict]}, mdb)["result"] != "errors"
    if mdb.get_collection(name="biosample_set").find_one({"id": biosample_id}) is None:
        mdb.get_collection(name="biosample_set").insert_one(biosample_dict)
        fakes.add("biosample")

    data_generation_id = "nmdc:omprc-11-nmtj1g51"
    data_generation_dict = {
        "id": data_generation_id,
        "has_input": [biosample_id],
        "type": "nmdc:NucleotideSequencing",
        "analyte_category": "metagenome",
        "associated_studies": [study_id],
    }
    assert (
        validate_json({"data_generation_set": [data_generation_dict]}, mdb)["result"]
        != "errors"
    )
    if (
        mdb.get_collection(name="data_generation_set").find_one(
            {"id": data_generation_id}
        )
        is None
    ):
        mdb.get_collection(name="data_generation_set").insert_one(data_generation_dict)
        fakes.add("data_generation")

    data_object_id = "nmdc:dobj-11-cpv4y420"
    data_object_dict = {
        "id": data_object_id,
        "name": "Raw sequencer read data",
        "description": "Metagenome Raw Reads for nmdc:omprc-11-nmtj1g51",
        "type": "nmdc:DataObject",
    }
    assert (
        validate_json({"data_object_set": [data_object_dict]}, mdb)["result"]
        != "errors"
    )
    if (
        mdb.get_collection(name="data_object_set").find_one({"id": data_object_id})
        is None
    ):
        mdb.get_collection(name="data_object_set").insert_one(data_object_dict)
        fakes.add("data_object")

    workflow_execution_id = "nmdc:wfmsa-11-fqq66x60.1"
    workflow_execution_dict = {
        "id": workflow_execution_id,
        "started_at_time": "2023-03-24T02:02:59.479107+00:00",
        "ended_at_time": "2023-03-24T02:02:59.479129+00:00",
        "was_informed_by": data_generation_id,
        "execution_resource": "JGI",
        "git_url": "https://github.com/microbiomedata/RawSequencingData",
        "has_input": [biosample_id],
        "has_output": [data_object_id],
        "type": "nmdc:MetagenomeSequencing",
    }
    assert (
        validate_json({"workflow_execution_set": [workflow_execution_dict]}, mdb)[
            "result"
        ]
        != "errors"
    )
    if (
        mdb.get_collection(name="workflow_execution_set").find_one(
            {"id": workflow_execution_id}
        )
        is None
    ):
        mdb.get_collection(name="workflow_execution_set").insert_one(
            workflow_execution_dict
        )
        fakes.add("workflow_execution")

    # Update the `alldocs` collection, which is a cache used by the endpoint under test.
    ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    # Confirm the endpoint responds with the data object we inserted above.
    response = api_site_client.request("GET", f"/data_objects/study/{study_id}")
    assert response.status_code == 200
    data_objects_by_biosample = response.json()
    assert any(
        biosample_data_objects["biosample_id"] == biosample_id
        and any(
            do["id"] == data_object_id for do in biosample_data_objects["data_objects"]
        )
        for biosample_data_objects in data_objects_by_biosample
    )

    # Clean up: Delete the documents we created within this test, from the database.
    if "study" in fakes:
        mdb.get_collection(name="study_set").delete_one({"id": study_id})
    if "biosample" in fakes:
        mdb.get_collection(name="biosample_set").delete_one({"id": biosample_id})
    if "data_generation":
        mdb.get_collection(name="data_generation_set").delete_one(
            {"id": data_generation_id}
        )
    if "data_object" in fakes:
        mdb.get_collection(name="data_object_set").delete_one({"id": data_object_id})
    if "workflow_execution" in fakes:
        mdb.get_collection(name="workflow_execution_set").delete_one(
            {"id": workflow_execution_id}
        )

    mdb.get_collection(name="alldocs").delete_many({})


def test_find_planned_processes(api_site_client):
    mdb = get_mongo_db()
    database_dict = json.loads(
        (REPO_ROOT_DIR / "tests" / "files" / "planned_processes.json").read_text()
    )
    for collection_name, docs in database_dict.items():
        for doc in docs:
            mdb[collection_name].replace_one({"id": doc["id"]}, doc, upsert=True)

    rv = api_site_client.request(
        "GET",
        "/planned_processes",
    )
    assert rv.json()["meta"]["count"] >= 9


def test_find_planned_process_by_id(api_site_client):
    # Seed the database with documents that represent instances of the `PlannedProcess` class or any of its subclasses.
    mdb = get_mongo_db()
    database_dict = json.loads(
        (REPO_ROOT_DIR / "tests" / "files" / "planned_processes.json").read_text()
    )
    for collection_name, docs in database_dict.items():
        for doc in docs:
            mdb[collection_name].replace_one({"id": doc["id"]}, doc, upsert=True)

    # Also, include a document that represents a `Study` (which is not a subclass of `PlannedProcess`),
    # so we can check whether the endpoint-under-test only searches collections that we expect it to.
    my_study = {"id": "nmdc:sty-1-foobar", "type": "nmdc:Study"}
    mdb.get_collection(name="study_set").replace_one(my_study, my_study, upsert=True)

    # Test case: The `id` belongs to a document that represents an instance of
    #            the `PlannedProcess` class or one of its subclasses.
    rv = api_site_client.request(
        "GET",
        f"/planned_processes/nmdc:wfmag-11-00jn7876.1",
    )
    planned_process = rv.json()
    assert "_id" not in planned_process
    assert planned_process["id"] == "nmdc:wfmag-11-00jn7876.1"

    # Test case: The `id` does not belong to a document.
    with pytest.raises(requests.exceptions.HTTPError):
        api_site_client.request(
            "GET",
            f"/planned_processes/nmdc:wfmag-11-00jn7876.99",
        )

    # Test case: The `id` belongs to a document, but that document does not represent
    #            an instance of the `PlannedProcess` class or any of its subclasses.
    with pytest.raises(requests.exceptions.HTTPError):
        api_site_client.request(
            "GET",
            f"/planned_processes/nmdc:sty-11-00000001",
        )


def _test_run_query_find_as(client):
    mdb = get_mongo_db()
    if not mdb.biosample_set.find_one({"id": "nmdc:bsm-12-7mysck21"}):
        mdb.biosample_set.insert_one(
            json.loads(
                (
                    REPO_ROOT_DIR / "tests" / "files" / "nmdc_bsm-12-7mysck21.json"
                ).read_text()
            )
        )

    # Make sure user client works
    response = client.request(
        "POST",
        "/queries:run",
        {"find": "biosample_set", "filter": {"id": "nmdc:bsm-12-7mysck21"}},
    )
    assert response.status_code == 200
    assert "cursor" in response.json()


def test_run_query_find_as_user(api_user_client):
    _test_run_query_find_as(api_user_client)


def test_run_query_find_as_site(api_site_client):
    _test_run_query_find_as(api_site_client)


def _test_run_query_delete_as(client):
    client_id_attribute = (
        "username" if isinstance(client, RuntimeApiUserClient) else "client_id"
    )

    mdb = get_mongo_db()
    biosample_id = "nmdc:bsm-12-deleteme"

    if not mdb.biosample_set.find_one({"id": biosample_id}):
        mdb.biosample_set.insert_one({"id": biosample_id})

    # Access should not work without permissions
    mdb["_runtime"].api.allow.delete_many(
        {
            "username": getattr(client, client_id_attribute),
            "action": "/queries:run(query_cmd:DeleteCommand)",
        }
    )
    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        response = client.request(
            "POST",
            "/queries:run",
            {
                "delete": "biosample_set",
                "deletes": [{"q": {"id": biosample_id}, "limit": 1}],
            },
        )
    assert excinfo.value.response.status_code == 403

    # Add persmissions to DB
    mdb["_runtime"].api.allow.insert_one(
        {
            "username": getattr(client, client_id_attribute),
            "action": "/queries:run(query_cmd:DeleteCommand)",
        }
    )
    try:
        response = client.request(
            "POST",
            "/queries:run",
            {
                "delete": "biosample_set",
                "deletes": [{"q": {"id": biosample_id}, "limit": 1}],
            },
        )
        assert response.status_code == 200
        assert response.json()["n"] == 1
    finally:
        mdb["_runtime"].api.allow.delete_one(
            {"username": getattr(client, client_id_attribute)}
        )


def test_run_query_delete_as_user(api_user_client):
    _test_run_query_delete_as(api_user_client)


def test_run_query_delete_as_site(api_site_client):
    _test_run_query_delete_as(api_site_client)


def test_run_query_update_as_user(api_user_client):
    """Submit a request to store data that does not comply with the schema."""
    mdb = get_mongo_db()
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)
    with pytest.raises(requests.HTTPError):
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "update": "biosample_set",
                "updates": [
                    {
                        "q": {"id": "nmdc:bsm-13-amrnys72"},
                        "u": {"$set": {"associated_studies.0": 42}},
                    }
                ],
            },
        )


def test_run_query_find__first_cursor_id_is_null_when_first_batch_is_empty(api_user_client):
    r"""
    Note: In this test, the client uses the "find" command to get an empty batch of studies.
          Since that batch contains fewer items than the batch size, its `cursor.id` is null.
    """

    study_id = "nmdc:sty-00-000001"

    # Confirm the study does not exist.
    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    assert study_set.count_documents({"id": study_id}) == 0

    # Attempt to get that study via a "find" command.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"id": study_id},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 0
    assert cursor["id"] is None  # i.e., no more batches to fetch


def test_run_query_find__first_cursor_id_is_null_when_first_batch_is_partially_full(api_user_client):
    r"""
    Note: In this test, the client uses the "find" command to get a batch of studies.
          Since that batch contains fewer items than the batch size, its `cursor.id` is null.
    """

    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")

    # Assert that the `study_set` collection does not already contain studies
    # like the ones we're going to generate here. Then, generate 6 studies
    # and insert them into the database.
    #
    # Note: The reason assertion is necessary is that some longstanding tests
    #       in this repostory leave "residue" in the test database after they
    #       run. The reason we do not just empty out the collections here is
    #       that some other longstanding tests in this repository rely on that
    #       "residue" (i.e. leftover documents) being there. This coupling
    #       between tests has made maintaining this repository's test suite
    #       difficult for some contributors. Rather than address the root cause
    #       right now, we will add the following TODO/FIXME comment about it.
    #
    # FIXME: Update tests that leave residue in the database to not do so, and
    #        update the "dependent" tests accordingly. Or, (preferrably)
    #        implement a database test fixture that always provides a "clean"
    #        (i.e. re-initialized) database to each test that uses the fixture.
    #        This could be scoped to the test function or to the test module,
    #        the latter being an option for people that _do_ want specific
    #        tests to be coupled to one another. This test fixture approach
    #        would also accommodate tests that abort prematurely due to
    #        failure and are, as a result, unable to run their "clean up" code.
    #
    study_title = "My study"
    assert study_set.count_documents({"title": study_title}) == 0
    
    # Seed the `study_set` collection with 6 documents.
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)

    # Attempt to get those studies via a "find" command, using a larger batch size.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 10,  # exceeds the number of studies
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert cursor["id"] is None  # i.e., no more batches to fetch

    # 🧹 Clean up / "Leave no trace" / "Pack it in, pack it out".
    study_set.delete_many({"title": study_title})


def test_run_query_find__first_cursor_id_is_string_when_first_batch_is_full(api_user_client):
    r"""
    Note: In this test, the client uses the "find" command to get a full batch of studies.
          Since that batch has no vacancy, its `cursor.id` is a string instead of null.
          Even though the next batch would be empty, the endpoint doesn't "know" that
          ahead of time and, so, will return a non-null `cursor.id` value. This is a
          limitation of the endpoint's pagination implementation.
    """

    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")

    # Assert that the `study_set` collection does not already contain studies
    # like the ones we're going to generate here. Then, generate 6 studies
    # and insert them into the database.
    #
    # Note: The reason assertion is necessary is that some longstanding tests
    #       in this repostory leave "residue" in the test database after they
    #       run. See "FIXME" comments in this module for more details.
    #
    study_title = "My study"
    assert study_set.count_documents({"title": study_title}) == 0
    
    # Seed the `study_set` collection with 6 documents.
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)

    # Attempt to get those studies via a "find" command, using an equal batch size.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 6,  # equals the number of studies
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert cursor["id"] is not None  # i.e., there are more batches to fetch

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_run_query_find__second_cursor_id_is_null_when_second_batch_is_empty(api_user_client):
    r"""
    Note: In this test, the client uses the "find" command to get one batch of studies,
          then uses the "getMore" command to get a second batch of studies, which is empty.
          Since the second batch contains fewer items than the batch size, its `cursor.id` is null.
    """

    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")

    # Assert that the `study_set` collection does not already contain studies
    # like the ones we're going to generate here. Then, generate 6 studies
    # and insert them into the database.
    #
    # Note: The reason assertion is necessary is that some longstanding tests
    #       in this repostory leave "residue" in the test database after they
    #       run. See "FIXME" comments in this module for more details.
    #
    study_title = "My study"
    assert study_set.count_documents({"title": study_title}) == 0
    
    # Seed the `study_set` collection with 6 documents.
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)

    # Fetch the first batch.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 6,  # equal to the number of studies
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert cursor["id"] is not None  # i.e., there is another batch to fetch

    # Fetch the second batch, which is empty.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 0
    assert cursor["id"] is None  # i.e., there is not another batch to fetch

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_run_query_find__second_cursor_id_is_null_when_second_batch_is_partially_full(api_user_client):
    r"""
    Note: In this test, the client uses the "find" command to get one batch of studies,
          then uses the "getMore" command to get a second batch of studies. Since the
          second batch contains fewer items than the batch size, its `cursor.id` is null.
    """

    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")

    # Assert that the `study_set` collection does not already contain studies
    # like the ones we're going to generate here. Then, generate 6 studies
    # and insert them into the database.
    #
    # Note: The reason assertion is necessary is that some longstanding tests
    #       in this repostory leave "residue" in the test database after they
    #       run. See "FIXME" comments in this module for more details.
    #
    study_title = "My study"
    assert study_set.count_documents({"title": study_title}) == 0
    
    # Seed the `study_set` collection with 6 documents.
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)

    # Fetch the first batch.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 5,  # less than the number of studies
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    items_in_batch_1: list = cursor["batch"]
    assert len(items_in_batch_1) == 5
    assert cursor["id"] is not None  # i.e., there is another batch to fetch

    # Fetch the second batch.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    items_in_batch_2: list = cursor["batch"]
    assert len(items_in_batch_2) == 1
    assert cursor["id"] is None  # i.e., there is not another batch to fetch

    # Confirm the documents in the second batch are distinct from the ones in the first batch.
    # Note: We'll use the documents' `id` values to determine that.
    ids_in_batch_1 = [item["id"] for item in items_in_batch_1]
    ids_in_batch_2 = [item["id"] for item in items_in_batch_2]
    assert len(set(ids_in_batch_1).intersection(set(ids_in_batch_2))) == 0

    # Confirm each of the `id`s of the studies we seeded the database with,
    # exist in the studies we received from the API.
    seeded_study_ids = [study["id"] for study in studies]
    assert set(ids_in_batch_1 + ids_in_batch_2) == set(seeded_study_ids)

    # 🧹 Clean up / "Leave no trace" / "Pack it in, pack it out".
    study_set.delete_many({"title": study_title})


def test_run_query_find__second_cursor_id_is_string_when_second_batch_is_full(api_user_client):
    r"""
    Note: In this test, the client uses the "find" command to get two full batches of studies.
          Since the second batch has no vacancy, its `cursor.id` is a string instead of null.
          Even though the third batch would be empty, the endpoint doesn't "know" that
          ahead of time and, so, will return a non-null `cursor.id` value. This is a
          limitation of the endpoint's pagination implementation.
    """

    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")

    # Assert that the `study_set` collection does not already contain studies
    # like the ones we're going to generate here. Then, generate 6 studies
    # and insert them into the database.
    #
    # Note: The reason assertion is necessary is that some longstanding tests
    #       in this repostory leave "residue" in the test database after they
    #       run. See "FIXME" comments in this module for more details.
    #
    study_title = "My study"
    assert study_set.count_documents({"title": study_title}) == 0
    
    # Seed the `study_set` collection with 6 documents.
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)

    # Fetch the first batch.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 3,  # half the number of studies
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 3
    assert cursor["id"] is not None  # i.e., there is another batch to fetch

    # Fetch the second batch, which is empty.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 3
    assert cursor["id"] is not None  # i.e., there is another batch to fetch

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_run_query_aggregate__cursor_id_is_null_when_first_batch_is_empty(api_user_client):
    study_id = "nmdc:sty-00-000001"

    # Confirm the study does not exist.
    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    assert study_set.count_documents({"id": study_id}) == 0

    # Attempt to get that study via an "aggregate" command.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "aggregate": "study_set",
            "pipeline": [
                {
                    "$match": {
                        "id": study_id,
                    },
                },
            ]
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 0
    assert cursor["id"] is None  # i.e., no more batches to fetch


def test_run_query_aggregate__cursor_id_is_null_when_documents_lack__id_fields(api_user_client):
    r"""
    Note: This test is focused on the scenario where the documents produced by
          the output stage of an aggregation pipeline do not have an `_id` field.
    """
    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")

    # Assert that the `study_set` collection does not already contain studies
    # like the ones we're going to generate here. Then, generate 6 studies
    # and insert them into the database.
    #
    # Note: The reason assertion is necessary is that some longstanding tests
    #       in this repostory leave "residue" in the test database after they
    #       run. See "FIXME" comments in this module for more details.
    #
    study_title = "My study"
    assert study_set.count_documents({"title": study_title}) == 0
    
    # Seed the `study_set` collection with 6 documents.
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)

    # Fetch the first batch and confirm the `cursor.id` value is null, since
    # pagination is not implemented when any of the documents lacks an `_id`
    # field.
    #
    # TODO: Consider whether an API client might construct an aggregation
    #       pipeline in which the output documents have custom `_id` values,
    #       as opposed to the default `_id` values that MongoDB generates.
    #
    # FIXME: The endpoint-under-test does not handle this scenario yet, so we do
    #        not assert anything about its response yet. Update the endpoint to
    #        handle this scenario gracefully; then update this test to assert
    #        things about the endpoint's response.
    #
    with pytest.raises(requests.exceptions.HTTPError):
        response = api_user_client.request(
            "POST",
            "/queries:run",
            {
                "aggregate": "study_set",
                "pipeline": [
                    {
                        "$match": {
                            "title": study_title,
                        },
                    },
                    # Remove the `_id` field, thereby violating a pagination prerequisite.
                    {
                        "$unset": "_id",
                    }
                ],
                "cursor": {"batchSize": 5},
            },
        )

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_run_query_aggregate_with_continuation(api_user_client):
    r"""
    Note: In this test, we seed the database with studies and biosamples
          and then fetch the biosamples (via the "aggregate" command,
          followed by the "getMore" command) in 3 batches.
    """

    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    biosample_set = mdb.get_collection("biosample_set")

    # Assert that the `study_set` and `biosample_set` collections do not already
    # contain studies and biosamples like the ones we're going to generate here.
    # Then, generate 1 study and 10 biosamples and insert them into the database.
    #
    # Note: The reason we do the assertion is that some tests in this repository
    #       leave residue in the test database after they run. The reason we do
    #       not just empty out the collections is that some other tests in this
    #       repository rely on that residue being there. This lack of isolation
    #       has made adding tests to this repository difficult for some people.
    #       We added a TODO/FIXME comment above about addressing the root cause.
    #       In the meantime, we are just asserting that the documents this test
    #       relies on do not exist in the database yet.
    #
    study_id = "nmdc:sty-00-000001"
    biosample_samp_name = "Sample for testing"
    assert study_set.count_documents({"id": study_id}) == 0
    assert biosample_set.count_documents({"samp_name": biosample_samp_name}) == 0

    # Seed the `study_set` collection with 1 document and the `biosample_set`
    # collection with 10 documents that are associated with that `study_set`
    # document.
    studies = faker.generate_studies(1, id=study_id)
    biosamples = faker.generate_biosamples(10, 
                                           associated_studies=[study_id], 
                                           samp_name=biosample_samp_name)
    study_set.insert_many(studies)
    biosample_set.insert_many(biosamples)

    # Fetch the first batch of biosamples associated with the study.
    #
    # References:
    # - https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/
    # - https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
    # - https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceRoot/
    #
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "aggregate": "study_set",
            "pipeline": [
                # Match the study we want to find biosamples for.
                {
                    "$match": {
                        "id": study_id,
                    },
                },
                # Join the `biosample_set` collection with the `study_set`
                # collection.
                {
                    "$lookup": {
                        "from": "biosample_set",
                        "localField": "id",
                        "foreignField": "associated_studies",
                        "as": "biosamples",
                    }
                },
                # Use `$unwind` followed by `$replaceRoot` to make the
                # biosamples be the top-level items in the pipeline's output.
                {
                    "$unwind": "$biosamples",
                },
                {
                    "$replaceRoot": {
                        "newRoot": "$biosamples",
                    },
                },
            ],
            "cursor": {"batchSize": 5},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    items_in_batch_1: list = cursor["batch"]
    assert len(items_in_batch_1) == 5
    assert cursor["id"] is not None  # i.e., there is another batch to fetch

    # Fetch the second batch of biosamples associated with the study.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    items_in_batch_2: list = cursor["batch"]
    assert len(items_in_batch_2) == 5
    assert cursor["id"] is not None  # i.e., there is another batch to fetch
    
    # Fetch the third batch of biosamples associated with the study.
    # Note: This is an empty batch.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    items_in_batch_3: list = cursor["batch"]
    assert len(items_in_batch_3) == 0  # empty batch
    assert cursor["id"] is None  # i.e., there are no more batches to fetch

    # Confirm the documents in each batches are unique from one another.
    # Note: We'll use the documents' `id` values to determine that.
    ids_in_batch_1 = [item["id"] for item in items_in_batch_1]
    ids_in_batch_2 = [item["id"] for item in items_in_batch_2]
    ids_in_batch_3 = [item["id"] for item in items_in_batch_3]
    assert len(set(ids_in_batch_1).intersection(set(ids_in_batch_2))) == 0
    assert len(set(ids_in_batch_1).intersection(set(ids_in_batch_3))) == 0
    assert len(set(ids_in_batch_2).intersection(set(ids_in_batch_3))) == 0

    # Confirm each of the `id`s of the biosamples we seeded the database with,
    # exist in the biosamples we received from the API.
    seeded_biosample_ids = [biosample["id"] for biosample in biosamples]
    assert set(ids_in_batch_1 + ids_in_batch_2 + ids_in_batch_3) == set(seeded_biosample_ids)

    # 🧹 Clean up.
    study_set.delete_many({"id": study_id})
    biosample_set.delete_many({"samp_name": biosample_samp_name})
