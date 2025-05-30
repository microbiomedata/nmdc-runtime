import json
import os
import re
from typing import List
from unittest.mock import MagicMock

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
from nmdc_runtime.api.endpoints.find import find_related_objects_for_workflow_execution
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


# TODO: Is the 43 MB `tests/nmdcdb.test.archive.gz` file in the repository obsolete? If so, delete it.


def ensure_alldocs_collection_has_been_materialized(
    force_refresh_of_alldocs: bool = False,
):
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
def base_url() -> str:
    r"""Returns the base URL of the API."""

    base_url = os.getenv("API_HOST")
    assert isinstance(base_url, str), "Base URL is not defined"
    return base_url


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


def test_metadata_json_submit_rejects_document_containing_broken_reference(
    api_user_client,
):
    # Make sure the `study_set` collection` doesn't already contain documents
    # having the IDs we're going to be using in this test.
    nonexistent_study_id = "nmdc:sty-00-000001"
    my_study_id = "nmdc:sty-00-000002"
    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    assert (
        study_set.count_documents({"id": {"$in": [my_study_id, nonexistent_study_id]}})
        == 0
    )

    # Generate a study having one of those IDs, and have it _reference_ a non-existent study
    # having the other one of those IDs.
    faker = Faker()
    my_study = faker.generate_studies(
        1, id=my_study_id, part_of=[nonexistent_study_id]
    )[0]

    # 👤 Give the user permission to use this API endpoint if it doesn't already have
    # such permission.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    user_allowance = {
        "username": api_user_client.username,
        "action": "/metadata/json:submit",
    }
    user_was_not_allowed = allowances_collection.find_one(user_allowance) is None
    if user_was_not_allowed:
        allowances_collection.insert_one(user_allowance)

    # Submit an API request whose payload contains the study.
    #
    # Note: The `api_user_client.request` method raises an exception when
    #       the HTTP response is not a "success" response.
    #
    with pytest.raises(requests.exceptions.HTTPError) as exc:
        api_user_client.request(
            "POST",
            "/metadata/json:submit",
            {"study_set": [my_study]},
        )
    response = exc.value.response
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Assert that the "detail" property of the response payload contains the words "errors",
    # "study_set" (i.e. the problematic collection), and "part_of" (i.e. the problematic field).
    #
    # Note: The "detail" value is a string representation of the Python dictionary returned
    #       by the `validate_json` function (i.e. its keys are wrapped in single quotes,
    #       not double quotes). It is not a valid JSON string, so we cannot use `json.loads()`
    #       to parse it. I do not know whether that was by design. Maybe I am not accessing
    #       the exception's content in the way its designer intended.
    #
    assert "detail" in response.json()
    detail_str = response.json()["detail"]
    assert isinstance(detail_str, str)
    assert "errors" in detail_str
    assert "study_set" in detail_str
    assert "part_of" in detail_str

    # Assert that the `study_set` collection still does not contain the study we submitted.
    assert study_set.count_documents({"id": my_study_id}) == 0

    # 🧹 Clean up.
    if user_was_not_allowed:
        allowances_collection.delete_one(user_allowance)


# TODO: Add a test that demonstrates the "success" behavior of the `/metadata/json:submit` API endpoint.
#       Note that that behavior involves Dagster.


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


def test_post_workflows_workflow_executions_inserts_submitted_document(api_site_client):
    r"""
    In this test, we submit a workflow execution to the `/workflows/workflow_executions` API endpoint,
    and then confirm that that workflow execution has been inserted into the database.
    """

    # Generate a `workflow_execution_set` document and the other kinds of documents necessary
    # in order to have referential integrity (i.e. generate all "referenced" documents).
    faker = Faker()
    study = faker.generate_studies(quantity=1)[0]
    biosample = faker.generate_biosamples(quantity=1, associated_studies=[study["id"]])[0]
    data_object_a, data_object_b = faker.generate_data_objects(quantity=2)
    data_generation = faker.generate_nucleotide_sequencings(
        quantity=1,
        associated_studies=[study["id"]],
        has_input=[biosample["id"]]
    )[0]
    workflow_execution = faker.generate_metagenome_annotations(
        quantity=1,
        has_input=[data_object_a["id"]],
        has_output=[data_object_b["id"]],  # schema says field optional; but validator complains when absent
        was_informed_by=data_generation["id"],
    )[0]

    # Make sure the `study_set`, `biosample_set`, `data_object_set`, `data_generation_set`, and
    # `workflow_execution_set` collections don't already contain documents having those IDs.
    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    biosample_set = mdb.get_collection("biosample_set")
    data_object_set = mdb.get_collection("data_object_set")
    data_generation_set = mdb.get_collection("data_generation_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    assert study_set.count_documents({"id": study["id"]}) == 0
    assert biosample_set.count_documents({"id": biosample["id"]}) == 0
    assert data_object_set.count_documents({"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}) == 0
    assert data_generation_set.count_documents({"id": data_generation["id"]}) == 0
    assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 0

    # Insert the "referenced" documents into the database.
    study_set.insert_one(study)
    biosample_set.insert_one(biosample)
    data_object_set.insert_many([data_object_a, data_object_b])
    data_generation_set.insert_one(data_generation)

    # Submit an API request whose payload contains the `workflow_execution_set` document.
    request_payload = {"workflow_execution_set": [workflow_execution]}
    response = api_site_client.request(
        "POST",
        "/workflows/workflow_executions",
        request_payload,
    )
    assert response.status_code == 200
    assert response.json() == {"message": "jobs accepted"}

    # Assert that the `workflow_execution_set` collection now contains the document we submitted.
    assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 1

    # 🧹 Clean up.
    study_set.delete_many({"id": study["id"]})
    biosample_set.delete_many({"id": biosample["id"]})
    data_object_set.delete_many({"id": {"$in": [data_object_a["id"], data_object_b["id"]]}})
    data_generation_set.delete_many({"id": data_generation["id"]})
    workflow_execution_set.delete_many({"id": workflow_execution["id"]})


def test_post_workflows_workflow_executions_rejects_document_containing_broken_reference(api_site_client):
    r"""
    In this test, we submit a workflow execution that contains a reference to a non-existent data generation,
    to the `/workflows/workflow_executions` API endpoint, and confirm the endpoint returns an error response.
    """

    # Generate a `data_object_set` document and generate a `workflow_execution_set` document that references
    # (a) that `data_object_set` document and (b) a non-existent `data_generation_set` document.
    faker = Faker()
    nonexistent_data_generation_id = "nmdc:dgns-00-000001"
    data_object_a, data_object_b = faker.generate_data_objects(quantity=2)
    workflow_execution = faker.generate_metagenome_annotations(
        quantity=1,
        has_input=[data_object_a["id"]],
        has_output=[data_object_b["id"]],  # schema says field optional; but validator complains when absent
        was_informed_by=nonexistent_data_generation_id,  # intentionally-broken reference
    )[0]
    
    # Make sure the `workflow_execution_set`, `data_generation_set`, and `data_object_set` collections
    # don't already contain documents like the ones involved in this test.
    mdb = get_mongo_db()
    data_generation_set = mdb.get_collection("data_generation_set")
    data_object_set = mdb.get_collection("data_object_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    assert data_generation_set.count_documents({"id": nonexistent_data_generation_id}) == 0
    assert data_object_set.count_documents({"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}) == 0
    assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 0

    # Insert the referenced `data_object_set` documents into the database. Notice that we are
    # not inserting any `data_generation_set` documents into the database.
    data_object_set.insert_many([data_object_a, data_object_b])

    # Submit an API request whose payload contains the `workflow_execution_set` document, which
    # contains a broken reference.
    request_payload = {"workflow_execution_set": [workflow_execution]}
    with pytest.raises(requests.exceptions.HTTPError) as exc:
        api_site_client.request(
            "POST",
            "/workflows/workflow_executions",
            request_payload,
        )
    response = exc.value.response
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Assert that the "detail" property of the response payload contains the words "errors",
    # "workflow_execution_set" (i.e. the problematic collection), and "was_informed_by"
    # (i.e. the problematic field), but not "has_input" or "has_output" (i.e. referring
    # fields that do not have any referential integrity issues).
    assert "detail" in response.json()
    detail_str = response.json()["detail"]
    assert isinstance(detail_str, str)
    assert "errors" in detail_str
    assert "workflow_execution_set" in detail_str
    assert "was_informed_by" in detail_str
    assert "has_input" not in detail_str
    assert "has_output" not in detail_str

    # Assert that the `workflow_execution_set` collection still does not contain the document we submitted.
    assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 0

    # 🧹 Clean up.
    data_object_set.delete_many({"id": {"$in": [data_object_a["id"], data_object_b["id"]]}})


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

def test_run_query_aggregate_as_user(api_user_client):
    """
    Submit a request to aggregate data without the correct permissions. Then add the permissions
    and submit the same request again, this time expecting a successful response.
    """
    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:AggregateCommand)",
    }
    # Assert that the `study_set` collection does not already contain studies
    # like the ones we're going to generate here. Then, generate 6 studies
    # and insert them into the database.
    #
    # Note: The reason assertion is necessary is that some longstanding tests
    #       in this repostory leave "residue" in the test database after they
    #       run. See "FIXME" comments in this module for more details.
    #
    # TODO: Use test fixtures to seed/cleanup the db
    study_title = "My study"
    assert study_set.count_documents({"title": study_title}) == 0
    # assert the user does not have permissions to run aggregate queries
    assert allowances_collection.count_documents(allow_spec) == 0

    # Seed the `study_set` collection with 6 documents.
    faker = Faker()
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)
    # Test case 1: when a user has no permission to run an aggregation query
    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
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
                ],
                "cursor": {"batchSize": 5},
            },
        )
    assert excinfo.value.response.status_code == 403

    # Test case 2: give user permission to run aggregate queries
    allowances_collection.replace_one(allow_spec, allow_spec, upsert=True)
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
            ],
            "cursor": {"batchSize": 5},
        },
    )
    assert response.status_code == 200
    # 🧹 Clean up. We use the same filters as in our initial absence check (above).
    study_set.delete_many({"title": study_title})
    allowances_collection.delete_many(allow_spec)


def test_find_related_objects_for_workflow_execution__returns_404_if_wfe_nonexistent(
    base_url: str,
):
    r"""
    Note: This test is focused on the case where there is no `WorkflowExecution` having the specified `id`.
    """

    workflow_execution_id = "nmdc:wfe-00-000000"  # arbitrary, made-up `id` value

    # Confirm there is no `WorkflowExecution` having that `id` value.
    # Note: This confirmation step is necessary because some old tests currently leave "residue" in the database.
    mdb = get_mongo_db()
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    assert workflow_execution_set.count_documents({"id": workflow_execution_id}) == 0

    # Confirm the endpoint response with an HTTP 404 status code.
    response = requests.request(
        "GET",
        url=f"{base_url}/workflow_executions/{workflow_execution_id}/related_resources",
    )
    assert response.status_code == 404


def test_find_related_objects_for_workflow_execution__returns_related_objects(
    base_url: str,
):
    # Generate interrelated documents.
    faker = Faker()
    study_a, study_b = faker.generate_studies(2)  # only one is related
    biosample_a, biosample_b = faker.generate_biosamples(
        2, associated_studies=[study_a["id"]]
    )  # both are related
    data_generation = faker.generate_nucleotide_sequencings(
        1,
        associated_studies=[study_a["id"]],
        has_input=[biosample_a["id"], biosample_b["id"]],
    )[0]
    data_object = faker.generate_data_objects(
        1, was_generated_by=data_generation["id"]
    )[0]
    workflow_execution = faker.generate_metagenome_annotations(
        1, was_informed_by=data_generation["id"], has_input=[data_object["id"]]
    )[0]

    # Confirm documents having the above-generated IDs don't already exist in the database.
    # Note: This absence check is necessary because some old tests currently leave "residue" in the database.
    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    biosample_set = mdb.get_collection("biosample_set")
    data_generation_set = mdb.get_collection("data_generation_set")
    data_object_set = mdb.get_collection("data_object_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    assert (
        study_set.count_documents({"id": {"$in": [study_a["id"], study_b["id"]]}}) == 0
    )
    assert (
        biosample_set.count_documents(
            {"id": {"$in": [biosample_a["id"], biosample_b["id"]]}}
        )
        == 0
    )
    assert data_generation_set.count_documents({"id": data_generation["id"]}) == 0
    assert data_object_set.count_documents({"id": data_object["id"]}) == 0
    assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 0

    # Insert the documents.
    study_set.insert_many([study_a, study_b])
    biosample_set.insert_many([biosample_a, biosample_b])
    data_generation_set.insert_one(data_generation)
    data_object_set.insert_one(data_object)
    workflow_execution_set.insert_one(workflow_execution)

    # Since we know the API endpoint depends upon the "alldocs" cache (collection),
    # refresh that cache since we just now updated the source of truth collections.
    ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    # Submit the API request and verify the response payload contains what we expect.
    response = requests.request(
        "GET",
        url=f"{base_url}/workflow_executions/{workflow_execution['id']}/related_resources",
    )
    assert response.status_code == 200
    response_payload = response.json()
    assert response_payload["workflow_execution_id"] == workflow_execution["id"]
    assert len(response_payload["data_objects"]) == 1
    assert response_payload["data_objects"][0]["id"] == data_object["id"]
    assert len(response_payload["related_workflow_executions"]) == 0
    assert len(response_payload["biosamples"]) == 2
    returned_biosample_ids = [b["id"] for b in response_payload["biosamples"]]
    assert set(returned_biosample_ids) == {
        biosample_a["id"],
        biosample_b["id"],
    }  # each id is present
    assert len(response_payload["studies"]) == 1  # only one study is related
    assert response_payload["studies"][0]["id"] == study_a["id"]  # not study_b

    # 🧹 Clean up. We use the same filters as in our initial absence check (above).
    study_set.delete_many({"id": {"$in": [study_a["id"], study_b["id"]]}})
    biosample_set.delete_many({"id": {"$in": [biosample_a["id"], biosample_b["id"]]}})
    data_generation_set.delete_many({"id": data_generation["id"]})
    data_object_set.delete_many({"id": data_object["id"]})
    workflow_execution_set.delete_many({"id": workflow_execution["id"]})


def test_find_related_objects_for_workflow_execution__returns_related_workflow_exections(
    base_url: str,
):
    """
    Note: This test is focused on the case where there _is_ a `WorkflowExecution` related to the one whose `id` is in the request URL.
          The related `WorkflowExecution` is part of the same `has_input` / `has_output` provenance chain as the specified one.
    """
    # Generate interrelated documents.
    faker = Faker()
    study_a = faker.generate_studies(1)[0]
    biosample_a = faker.generate_biosamples(1, associated_studies=[study_a["id"]])[0]
    data_generation_a = faker.generate_nucleotide_sequencings(
        1, associated_studies=[study_a["id"]], has_input=[biosample_a["id"]]
    )[0]
    data_object_a = faker.generate_data_objects(
        1, was_generated_by=data_generation_a["id"]
    )[0]
    workflow_execution_a = faker.generate_metagenome_annotations(
        1, was_informed_by=data_generation_a["id"], has_input=[data_object_a["id"]]
    )[0]

    # Create a second `WorkflowExecution` that is related to the first one.
    data_object_b = faker.generate_data_objects(
        1, has_output=workflow_execution_a["id"]
    )[0]
    workflow_execution_b = faker.generate_metagenome_annotations(
        1,
        was_informed_by=data_generation_a["id"],
        has_input=[data_object_b["id"]],
    )[0]

    # Confirm documents having the above-generated IDs don't already exist in the database.
    # Note: This absence check is necessary because some old tests currently leave "residue" in the database.
    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    biosample_set = mdb.get_collection("biosample_set")
    data_generation_set = mdb.get_collection("data_generation_set")
    data_object_set = mdb.get_collection("data_object_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    assert study_set.count_documents({"id": study_a["id"]}) == 0
    assert biosample_set.count_documents({"id": biosample_a["id"]}) == 0
    assert (
        data_generation_set.count_documents({"id": {"$in": [data_generation_a["id"]]}})
        == 0
    )
    assert (
        data_object_set.count_documents(
            {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
        )
        == 0
    )
    assert (
        workflow_execution_set.count_documents(
            {"id": {"$in": [workflow_execution_a["id"], workflow_execution_b["id"]]}}
        )
        == 0
    )

    # Insert the documents.
    study_set.insert_one(study_a)
    biosample_set.insert_one(biosample_a)
    data_generation_set.insert_many([data_generation_a])
    data_object_set.insert_many([data_object_a, data_object_b])
    workflow_execution_set.insert_many([workflow_execution_a, workflow_execution_b])

    # Since we know the API endpoint depends upon the "alldocs" cache (collection),
    # refresh that cache since we just now updated the source of truth collections.
    ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    # Submit the API request and verify the response payload contains what we expect.
    response = requests.request(
        "GET",
        url=f"{base_url}/workflow_executions/{workflow_execution_a['id']}/related_resources",
    )
    assert response.status_code == 200
    response_payload = response.json()
    assert response_payload["workflow_execution_id"] == workflow_execution_a["id"]
    assert len(response_payload["data_objects"]) == 2
    returned_data_object_ids = [do["id"] for do in response_payload["data_objects"]]
    assert set(returned_data_object_ids) == {
        data_object_a["id"],
        data_object_b["id"],
    }  # each id is present
    assert len(response_payload["related_workflow_executions"]) == 1
    assert (
        response_payload["related_workflow_executions"][0]["id"]
        == workflow_execution_b["id"]
    )
    assert len(response_payload["biosamples"]) == 1
    assert response_payload["biosamples"][0]["id"] == biosample_a["id"]
    assert len(response_payload["studies"]) == 1
    assert response_payload["studies"][0]["id"] == study_a["id"]

    # 🧹 Clean up. We use the same filters as in our initial absence check (above).
    study_set.delete_one({"id": study_a["id"]})
    biosample_set.delete_one({"id": biosample_a["id"]})
    data_generation_set.delete_many({"id": {"$in": [data_generation_a["id"]]}})
    data_object_set.delete_many(
        {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
    )
    workflow_execution_set.delete_many(
        {"id": {"$in": [workflow_execution_a["id"], workflow_execution_b["id"]]}}
    )


def test_run_query_find__first_batch_and_its_cursor_id(api_user_client):
    r"""
    Note: In this test, we seed the database, then we use the "find" command to fetch a single
          batch of results with a few different batch sizes. For each fetch, we assert that
          the items and `cursor.id` are what we expect.
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
    nonexistent_study_title = "Nonexistent study"
    assert study_set.count_documents({"title": study_title}) == 0
    assert study_set.count_documents({"title": nonexistent_study_title}) == 0

    # Seed the `study_set` collection with 6 documents.
    faker = Faker()
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)

    # Test case 1: When the first batch is empty, its `cursor.id` is null.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": nonexistent_study_title},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 0
    assert cursor["id"] is None

    # Test case 2: When the first batch is partially full, its `cursor.id` is null.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 10,
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert cursor["id"] is None

    # Test case 3: When the first batch is full (matching the total), its `cursor.id` is not null.
    #              Even though the next batch would be empty, the endpoint doesn't "know" that
    #              ahead of time and, so, will return a non-null `cursor.id` value. This is a
    #              limitation of the endpoint's pagination implementation.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 6,
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert isinstance(cursor["id"], str)

    # Test case 4: When the first batch is full (smaller than total), its `cursor.id` is not null.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 5,
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 5
    assert isinstance(cursor["id"], str)

    # 🧹 Clean up / "Leave no trace" / "Pack it in, pack it out".
    study_set.delete_many({"title": study_title})


def test_run_query_find__second_batch_and_its_cursor_id(api_user_client):
    r"""
    Note: In this test, we seed the database, then we use the "find" command to fetch a two
          batches of results with a few different batch sizes. For each fetch, we assert that
          the items and `cursor.id` are what we expect.
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
    faker = Faker()
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)

    # Test case 1: When the second batch is empty, its `cursor.id` is null.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 6,
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert isinstance(cursor["id"], str)
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
    assert cursor["id"] is None

    # Test case 2: When the second batch is partially full, its `cursor.id` is null.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 5,
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 5
    assert isinstance(cursor["id"], str)
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 1
    assert cursor["id"] is None

    # Test case 3: When the second batch is full (and includes the final item), its `cursor.id` is not null.
    #              Even though the next batch would be empty, the endpoint doesn't "know" that
    #              ahead of time and, so, will return a non-null `cursor.id` value. This is a
    #              limitation of the endpoint's pagination implementation.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 3,
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 3
    assert isinstance(cursor["id"], str)
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
    assert isinstance(cursor["id"], str)

    # Test case 4: When the second batch is full (and does not include the final item), its `cursor.id` is not null.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 2,
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 2
    assert isinstance(cursor["id"], str)
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 2
    assert isinstance(cursor["id"], str)

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_run_query_find__three_batches_and_their_items(api_user_client):
    r"""
    Note: In this test, we fetch the results across 3 batches and verify
          the items are what we expect.
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

    # Seed the `study_set` collection with 10 documents.
    faker = Faker()
    studies = faker.generate_studies(10, title=study_title)
    study_set.insert_many(studies)

    # Fetch the first batch.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "study_set",
            "filter": {"title": study_title},
            "batchSize": 4,
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    items_in_batch_1: list = cursor["batch"]
    assert len(items_in_batch_1) == 4
    assert isinstance(cursor["id"], str)

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
    assert len(items_in_batch_2) == 4
    assert isinstance(cursor["id"], str)

    # Fetch the third batch.
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
    assert len(items_in_batch_3) == 2
    assert cursor["id"] is None

    # Confirm the documents in each batches are unique from one another.
    # Note: We'll use the documents' `id` values to determine that.
    ids_in_batch_1 = [item["id"] for item in items_in_batch_1]
    ids_in_batch_2 = [item["id"] for item in items_in_batch_2]
    ids_in_batch_3 = [item["id"] for item in items_in_batch_3]
    assert len(set(ids_in_batch_1).intersection(set(ids_in_batch_2))) == 0
    assert len(set(ids_in_batch_1).intersection(set(ids_in_batch_3))) == 0
    assert len(set(ids_in_batch_2).intersection(set(ids_in_batch_3))) == 0

    # Confirm each of the `id`s of the studies we seeded the database with,
    # exist in the studies we received from the API.
    seeded_study_ids = [study["id"] for study in studies]
    assert set(ids_in_batch_1 + ids_in_batch_2 + ids_in_batch_3) == set(
        seeded_study_ids
    )

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_run_query_aggregate__first_batch_and_its_cursor_id(api_user_client):
    r"""
    Note: In this test, we seed the database, then we use the "aggregate" command to fetch a single
          batch of results with a few different batch sizes. For each fetch, we assert that
          the items and `cursor.id` are what we expect.
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
    nonexistent_study_title = "Nonexistent study"
    assert study_set.count_documents({"title": study_title}) == 0
    assert study_set.count_documents({"title": nonexistent_study_title}) == 0

    # Seed the `study_set` collection with 6 documents.
    faker = Faker()
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)
    mdb = get_mongo_db()
    # give user permission to run aggregate queries
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:AggregateCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)
    # Test case 1: When the first batch is empty, its `cursor.id` is null.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "aggregate": "study_set",
            "pipeline": [
                {
                    "$match": {
                        "title": nonexistent_study_title,
                    },
                },
            ],
            "cursor": {"batchSize": 5},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 0
    assert cursor["id"] is None

    # Test case 2: When the first batch is partially full, its `cursor.id` is null.
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
            ],
            "cursor": {"batchSize": 10},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert cursor["id"] is None

    # Test case 3: When the first batch is full (matching the total), its `cursor.id` is not null.
    #              Even though the next batch would be empty, the endpoint doesn't "know" that
    #              ahead of time and, so, will return a non-null `cursor.id` value. This is a
    #              limitation of the endpoint's pagination implementation.
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
            ],
            "cursor": {"batchSize": 6},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert isinstance(cursor["id"], str)

    # Test case 4: When the first batch is full (smaller than total), its `cursor.id` is not null.
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
            ],
            "cursor": {"batchSize": 5},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 5
    assert isinstance(cursor["id"], str)

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_run_query_aggregate__second_batch_and_its_cursor_id(api_user_client):
    r"""
    Note: In this test, we seed the database, then we use the "aggregate" command to fetch a two
          batches of results with a few different batch sizes. For each fetch, we assert that
          the items and `cursor.id` are what we expect.
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
    faker = Faker()
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)
    # give user permission to run aggregate queries
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:AggregateCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)
    # Test case 1: When the second batch is empty, its `cursor.id` is null.
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
            ],
            "cursor": {"batchSize": 6},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 6
    assert isinstance(cursor["id"], str)
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
    assert cursor["id"] is None

    # Test case 2: When the second batch is partially full, its `cursor.id` is null.
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
            ],
            "cursor": {"batchSize": 5},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 5
    assert isinstance(cursor["id"], str)
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 1
    assert cursor["id"] is None

    # Test case 3: When the second batch is full (and includes the final item), its `cursor.id` is not null.
    #              Even though the next batch would be empty, the endpoint doesn't "know" that
    #              ahead of time and, so, will return a non-null `cursor.id` value. This is a
    #              limitation of the endpoint's pagination implementation.
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
            ],
            "cursor": {"batchSize": 3},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 3
    assert isinstance(cursor["id"], str)
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
    assert isinstance(cursor["id"], str)

    # Test case 4: When the second batch is full (and does not include the final item), its `cursor.id` is not null.
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
            ],
            "cursor": {"batchSize": 2},
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 2
    assert isinstance(cursor["id"], str)
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "getMore": cursor["id"],
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 2
    assert isinstance(cursor["id"], str)

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_run_query_aggregate__three_batches_and_their_items(api_user_client):
    r"""
    Note: In this test, we use a more complex aggregation pipeline, where we "join"
          studies and biosamples. We fetch the results across 3 batches and verify
          the items and `cursor.id` values are what we expect.
    """

    mdb = get_mongo_db()
    study_set = mdb.get_collection("study_set")
    biosample_set = mdb.get_collection("biosample_set")

    # Assert that the `study_set` and `biosample_set` collections do not already
    # contain studies and biosamples like the ones we're going to generate here.
    # Then, generate 1 study and 10 biosamples and insert them into the database.
    #
    # Note: The reason assertion is necessary is that some longstanding tests
    #       in this repostory leave "residue" in the test database after they
    #       run. See "FIXME" comments in this module for more details.
    #
    study_id = "nmdc:sty-00-000001"
    biosample_samp_name = "Sample for testing"
    assert study_set.count_documents({"id": study_id}) == 0
    assert biosample_set.count_documents({"samp_name": biosample_samp_name}) == 0

    # Seed the `study_set` collection with 1 document and the `biosample_set`
    # collection with 10 documents that are associated with that `study_set`
    # document.
    faker = Faker()
    studies = faker.generate_studies(1, id=study_id)
    biosamples = faker.generate_biosamples(
        10, associated_studies=[study_id], samp_name=biosample_samp_name
    )
    study_set.insert_many(studies)
    biosample_set.insert_many(biosamples)
    # give user permission to run aggregate queries
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:AggregateCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)
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
    assert isinstance(cursor["id"], str)

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
    assert isinstance(cursor["id"], str)

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
    assert set(ids_in_batch_1 + ids_in_batch_2 + ids_in_batch_3) == set(
        seeded_biosample_ids
    )

    # 🧹 Clean up.
    study_set.delete_many({"id": study_id})
    biosample_set.delete_many({"samp_name": biosample_samp_name})


def test_run_query_aggregate__cursor_id_is_null_when_any_document_lacks_underscore_id_field(
    api_user_client,
):
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
    faker = Faker()
    studies = faker.generate_studies(6, title=study_title)
    study_set.insert_many(studies)
    # give user permission to run aggregate queries
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:AggregateCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)
    # Fetch the first batch of 5 and confirm the `cursor.id` is null,
    # even though we didn't receive all 6 items.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "aggregate": "study_set",
            "pipeline": [
                # Only get the 6 studies we inserted above.
                {
                    "$match": {
                        "title": study_title,
                    },
                },
                # In the final stage of the pipeline, we remove the `_id` field,
                # upon which the pagination algorithm relies.
                {
                    "$unset": "_id",
                },
            ],
            "cursor": {"batchSize": 5},  # less than the number of studies
        },
    )
    assert response.status_code == 200
    cursor = response.json()["cursor"]
    assert len(cursor["batch"]) == 5  # the client only gets the first batch
    assert cursor["id"] is None  # the client is not given an opportunity to paginate

    # 🧹 Clean up.
    study_set.delete_many({"title": study_title})


def test_release_job(api_site_client):
    mdb = get_mongo_db()
    test_job_id = mdb.jobs.find_one({"workflow.id": "test"})["id"]
    # claim the test job
    api_site_client.request(
        "POST",
        f"/jobs/{test_job_id}:claim",
    )
    # release the test job
    rv = api_site_client.request(
        "POST",
        f"/jobs/{test_job_id}:release",
    )
    # assert that all claims by this site are cancelled.
    assert all(
        claim["cancelled"]
        for claim in rv.json()["claims"]
        if claim["site_id"] == api_site_client.site_id
    )
