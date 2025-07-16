import json
import os
import re

import pytest
import requests
from dagster import build_op_context
from nmdc_runtime.config import IS_RELATED_IDS_ENDPOINT_ENABLED
from starlette import status
from tenacity import wait_random_exponential, stop_after_attempt, retry
from toolz import get_in

from nmdc_runtime.api.core.auth import get_password_hash
from nmdc_runtime.api.core.metadata import df_from_sheet_in, _validate_changesheet
from nmdc_runtime.api.core.util import generate_secret, dotted_path_for
from nmdc_runtime.api.db.mongo import get_mongo_db, validate_json
from nmdc_runtime.api.endpoints.util import (
    persist_content_and_get_drs_object,
    strip_oid,
)
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
from nmdc_runtime.util import REPO_ROOT_DIR, ensure_unique_id_indexes
from tests.lib.faker import Faker


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

def test_queries_run_invalid_update(api_user_client):
    # Seed the database
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    allowances_collection.replace_one(allow_spec, allow_spec, upsert=True)
    faker = Faker()
    study_set = mdb.get_collection("study_set")
    study = faker.generate_studies(1)[0]
    assert study_set.count_documents({"id": study["id"]}) == 0
    study_set.insert_one(study)
    
    # test incorrect update - initial command syntax that brought this issue to light
    with pytest.raises(requests.HTTPError) as exc_info:
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "update": "study_set",
                "updates": [
                    {
                        "q": {"id": "nmdc:sty-11-hhkbcg72"},
                        "u": {"$unset": "notes"},
                    }
                ],
            },
        )
    expected_response = {
        "detail": [
            {
                "index": 0,
                "code": 9,
                "errmsg": 'Modifiers operate on fields but we found type string instead. For example: {$mod: {<field>: ...}} not {$unset: "notes"}',
            }
        ]
    }
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert exc_info.value.response.json() == expected_response
    
    # test incorrect delete
    with pytest.raises(requests.HTTPError) as exc_info:
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "delete": "study_set",
                "deletes": [{"id": "nmdc:sty-11-hhkbcg72"}]

            },
        )
    expected_response = {'detail': [{'type': 'missing', 'loc': ['body', 'FindCommand', 'find'], 'msg': 'Field required', 'input': {'delete': 'study_set', 'deletes': [{'id': 'nmdc:sty-11-hhkbcg72'}]}}, {'type': 'missing', 'loc': ['body', 'AggregateCommand', 'aggregate'], 'msg': 'Field required', 'input': {'delete': 'study_set', 'deletes': [{'id': 'nmdc:sty-11-hhkbcg72'}], 'cursor': {'batchSize': 25}}}, {'type': 'missing', 'loc': ['body', 'AggregateCommand', 'pipeline'], 'msg': 'Field required', 'input': {'delete': 'study_set', 'deletes': [{'id': 'nmdc:sty-11-hhkbcg72'}], 'cursor': {'batchSize': 25}}}, {'type': 'missing', 'loc': ['body', 'GetMoreCommand', 'getMore'], 'msg': 'Field required', 'input': {'delete': 'study_set', 'deletes': [{'id': 'nmdc:sty-11-hhkbcg72'}]}}, {'type': 'missing', 'loc': ['body', 'CollStatsCommand', 'collStats'], 'msg': 'Field required', 'input': {'delete': 'study_set', 'deletes': [{'id': 'nmdc:sty-11-hhkbcg72'}]}}, {'type': 'missing', 'loc': ['body', 'CountCommand', 'count'], 'msg': 'Field required', 'input': {'delete': 'study_set', 'deletes': [{'id': 'nmdc:sty-11-hhkbcg72'}]}}, {'type': 'missing', 'loc': ['body', 'DeleteCommand', 'deletes', 0, 'q'], 'msg': 'Field required', 'input': {'id': 'nmdc:sty-11-hhkbcg72'}}, {'type': 'missing', 'loc': ['body', 'DeleteCommand', 'deletes', 0, 'limit'], 'msg': 'Field required', 'input': {'id': 'nmdc:sty-11-hhkbcg72'}}, {'type': 'missing', 'loc': ['body', 'UpdateCommand', 'update'], 'msg': 'Field required', 'input': {'delete': 'study_set', 'deletes': [{'id': 'nmdc:sty-11-hhkbcg72'}]}}, {'type': 'missing', 'loc': ['body', 'UpdateCommand', 'updates'], 'msg': 'Field required', 'input': {'delete': 'study_set', 'deletes': [{'id': 'nmdc:sty-11-hhkbcg72'}]}}]}
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert exc_info.value.response.json() == expected_response
    
    # 🧹 Clean up.
    allowances_collection.delete_many(allow_spec)
    study_set.delete_many({"id": study["id"]})


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
    biosample = faker.generate_biosamples(quantity=1, associated_studies=[study["id"]])[
        0
    ]
    data_object_a, data_object_b = faker.generate_data_objects(quantity=2)
    data_generation = faker.generate_nucleotide_sequencings(
        quantity=1, associated_studies=[study["id"]], has_input=[biosample["id"]]
    )[0]
    workflow_execution = faker.generate_metagenome_annotations(
        quantity=1,
        has_input=[data_object_a["id"]],
        has_output=[
            data_object_b["id"]
        ],  # schema says field optional; but validator complains when absent
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
    assert (
        data_object_set.count_documents(
            {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
        )
        == 0
    )
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
    data_object_set.delete_many(
        {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
    )
    data_generation_set.delete_many({"id": data_generation["id"]})
    workflow_execution_set.delete_many({"id": workflow_execution["id"]})


def test_post_workflows_workflow_executions_rejects_document_containing_broken_reference(
    api_site_client,
):
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
        has_output=[
            data_object_b["id"]
        ],  # schema says field optional; but validator complains when absent
        was_informed_by=nonexistent_data_generation_id,  # intentionally-broken reference
    )[0]

    # Make sure the `workflow_execution_set`, `data_generation_set`, and `data_object_set` collections
    # don't already contain documents like the ones involved in this test.
    mdb = get_mongo_db()
    data_generation_set = mdb.get_collection("data_generation_set")
    data_object_set = mdb.get_collection("data_object_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    assert (
        data_generation_set.count_documents({"id": nonexistent_data_generation_id}) == 0
    )
    assert (
        data_object_set.count_documents(
            {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
        )
        == 0
    )
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
    data_object_set.delete_many(
        {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
    )


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


@pytest.fixture
def fake_study_in_mdb():
    # Seed the database with a study that neither influences—nor is influenced by—any documents.
    mdb = get_mongo_db()
    faker = Faker()
    study_a = faker.generate_studies(quantity=1, part_of=[])[0]
    study_set = mdb.get_collection(name="study_set")
    assert study_set.count_documents({"id": study_a["id"]}) == 0
    study_set.insert_many([study_a])
    ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    yield study_a

    # 🧹 Clean up: Delete the study we created earlier.
    study_set.delete_many({"id": study_a["id"]})


@pytest.fixture
def fake_study_nonexistent_in_mdb():
    mdb = get_mongo_db()
    nonexistent_study_id = "nmdc:sty-00-00000x"
    assert (
        mdb.get_collection("study_set").count_documents({"id": nonexistent_study_id})
        == 0
    )
    yield nonexistent_study_id


@pytest.mark.skipif(
    not IS_RELATED_IDS_ENDPOINT_ENABLED, reason="Target endpoint is disabled"
)
def test_get_related_ids_returns_unsuccessful_status_code_when_any_subject_does_not_exist(
    api_user_client, fake_study_in_mdb, fake_study_nonexistent_in_mdb
):
    r"""
    This test demonstrates that the `/nmdcschema/related_ids` API endpoint returns an
    unsuccessful status code when the request contains an `id` that does not exist in the
    database; and that that is the case whether that `id` is submitted on its own or as
    part of a list of `id`s (even if some of the other `id`s in the list _do_ exist).
    """

    # Request the `id`s of documents related to only that nonexistent study.
    #
    # Note: The `api_user_client` fixture's `request` method will raise an
    #       exception if the server responds with an unsuccessful status code.
    #
    with pytest.raises(requests.exceptions.HTTPError):
        api_user_client.request(
            "GET",
            f"/nmdcschema/related_ids/ids={fake_study_nonexistent_in_mdb}/types=nmdc:NamedThing",
        )

    # Submit the same request, but specify _both_ the existing study's `id`
    # and the nonexistent study's `id`.
    with pytest.raises(requests.exceptions.HTTPError):
        api_user_client.request(
            "GET",
            (
                f'/nmdcschema/related_ids/ids={fake_study_in_mdb["id"]},{fake_study_nonexistent_in_mdb}'
                + "/types=nmdc:NamedThing"
            ),  # two ids
        )


@pytest.mark.skipif(
    not IS_RELATED_IDS_ENDPOINT_ENABLED, reason="Target endpoint is disabled"
)
def test_get_related_ids_returns_empty_resources_list_for_isolated_subject(
    api_user_client, fake_study_in_mdb
):
    # Request the `id`s of the documents that either influence—or are influenced by—that study.
    response = api_user_client.request(
        "GET",
        f'/nmdcschema/related_ids/ids={fake_study_in_mdb["id"]}/types=nmdc:NamedThing',
    )
    # Assert that the response contains an empty "resources" list.
    assert response.status_code == 200
    assert response.json() == {
        "resources": [
            {
                "id": fake_study_in_mdb["id"],
                "was_influenced_by": [],
                "influenced": [],
            }
        ]
    }


@pytest.fixture
def fake_studies_and_biosamples_in_mdb():
    # Seed the database with the following interrelated documents (represented
    # here as a Mermaid graph/flowchart within a Markdown fenced code block):
    # Docs: https://mermaid.js.org/syntax/flowchart.html
    r"""
    ```mermaid
    graph BT
        study_a
        study_b --> |part_of| study_a
        biosample_a --> |associated_studies| study_a
        biosample_b --> |associated_studies| study_b
    ```
    """
    mdb = get_mongo_db()
    faker = Faker()
    study_a = faker.generate_studies(1)[0]
    study_b = faker.generate_studies(1, part_of=[study_a["id"]])[0]
    biosample_a = faker.generate_biosamples(1, associated_studies=[study_a["id"]])[0]
    biosample_b = faker.generate_biosamples(1, associated_studies=[study_b["id"]])[0]
    study_ids = [study_a["id"], study_b["id"]]
    biosample_ids = [biosample_a["id"], biosample_b["id"]]
    study_set = mdb.get_collection(name="study_set")
    biosample_set = mdb.get_collection(name="biosample_set")
    assert study_set.count_documents({"id": {"$in": study_ids}}) == 0
    assert biosample_set.count_documents({"id": {"$in": biosample_ids}}) == 0
    study_set.insert_many([study_a, study_b])
    biosample_set.insert_many([biosample_a, biosample_b])
    ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    yield study_a, study_b, biosample_a, biosample_b

    # 🧹 Clean up: Delete the documents we created earlier.
    study_set.delete_many({"id": {"$in": study_ids}})
    biosample_set.delete_many({"id": {"$in": biosample_ids}})
    ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)


@pytest.mark.skipif(
    not IS_RELATED_IDS_ENDPOINT_ENABLED, reason="Target endpoint is disabled"
)
def test_get_related_ids_returns_related_ids(
    api_user_client, fake_studies_and_biosamples_in_mdb
):
    study_a, study_b, biosample_a, biosample_b = fake_studies_and_biosamples_in_mdb
    # Request the `id`s of the documents related to `study_a`, which is influenced by
    # `study_b`, `biosample_a`, and `biosample_b`, and which influences nothing.
    #
    # Note: The API doesn't advertise that the related `id`s will be in any particular order.
    #
    response = api_user_client.request(
        "GET",
        f'/nmdcschema/related_ids/ids={study_a["id"]}/types=nmdc:NamedThing',
    )
    assert response.status_code == 200
    response_resource = response.json()["resources"][0]
    assert study_a["id"] == response_resource["id"]
    assert {study_b["id"], biosample_a["id"], biosample_b["id"]} == set(
        [r["id"] for r in response_resource["was_influenced_by"]]
    )
    assert len(response_resource["influenced"]) == 0

    # Request the `id`s of the documents related to `study_b`, which is influenced by
    # `biosample_b`, and which influences `study_a`.
    response = api_user_client.request(
        "GET",
        f'/nmdcschema/related_ids/ids={study_b["id"]}/types=nmdc:NamedThing',
    )
    assert response.status_code == 200
    response_resource = response.json()["resources"][0]
    assert study_b["id"] == response_resource["id"]
    assert {biosample_b["id"]} == set(
        [r["id"] for r in response_resource["was_influenced_by"]]
    )
    assert {study_a["id"]} == set([r["id"] for r in response_resource["influenced"]])

    # Request the `id`s of the documents related to `biosample_a`, which influences `study_a`,
    # and is not influenced by anything.
    response = api_user_client.request(
        "GET",
        f'/nmdcschema/related_ids/ids={biosample_a["id"]}/types=nmdc:NamedThing',
    )
    assert response.status_code == 200
    response_resource = response.json()["resources"][0]
    assert biosample_a["id"] == response_resource["id"]
    assert len(response_resource["was_influenced_by"]) == 0
    assert {study_a["id"]} == set([r["id"] for r in response_resource["influenced"]])


class TestFindDataObjectsForStudy:
    r"""
    Tests targeting the `/data_objects/study/{study_id}` API endpoint.
    """

    # Constant IDs that the seeder can use and that seeded database-dependent tests can "expect."
    study_id = "nmdc:sty-00-000001"
    biosample_id = "nmdc:bsm-00-000001"
    data_generation_id = "nmdc:dgns-00-000001"
    data_object_ids = [
        "nmdc:dobj-00-000001",
        "nmdc:dobj-00-000002",
        "nmdc:dobj-00-000003",
    ]
    workflow_execution_ids = ["nmdc:wfmgan-00-000001", "nmdc:wfmgan-00-000002"]

    @pytest.fixture()
    def seeded_db(self):
        r"""
        Fixture that seeds the database, yields it, and then cleans it up.

        Note: Since this fixture is defined within a class, it is only accessible to tests
              defined within the same class.

        Here is a Mermaid graph/flowchart showing the documents that this fixture inserts into
        the database, and the relationships between those documents.
        Reference: https://mermaid.js.org/syntax/flowchart.html
        ```mermaid
        graph BT
            study
            biosample --> |associated_studies| study
            data_generation --> |associated_studies| study
            data_generation --> |has_input| biosample
            workflow_execution --> |has_input| biosample
            workflow_execution --> |has_output| data_object_a
            workflow_execution --> |has_output| data_object_b
            workflow_execution --> |was_informed_by| data_generation
            data_object_a
            data_object_b
        ```
        """

        faker = Faker()
        study = faker.generate_studies(quantity=1, id=self.study_id)[0]
        biosample = faker.generate_biosamples(
            quantity=1, id=self.biosample_id, associated_studies=[study["id"]]
        )[0]
        data_generation = faker.generate_nucleotide_sequencings(
            quantity=1,
            id=self.data_generation_id,
            associated_studies=[study["id"]],
            has_input=[biosample["id"]],
        )[0]
        data_object_a, data_object_b = faker.generate_data_objects(quantity=2)
        data_object_a["id"] = self.data_object_ids[0]
        data_object_b["id"] = self.data_object_ids[1]
        data_object_a["data_category"] = "instrument_data"
        data_object_b["data_category"] = "processed_data"
        workflow_execution = faker.generate_metagenome_annotations(
            quantity=1,
            id=self.workflow_execution_ids[0],
            has_input=[biosample["id"]],
            has_output=[data_object_a["id"], data_object_b["id"]],
            was_informed_by=data_generation["id"],
        )[0]

        mdb = get_mongo_db()
        study_set = mdb.get_collection(name="study_set")
        biosample_set = mdb.get_collection(name="biosample_set")
        data_generation_set = mdb.get_collection(name="data_generation_set")
        data_object_set = mdb.get_collection(name="data_object_set")
        workflow_execution_set = mdb.get_collection(name="workflow_execution_set")

        assert study_set.count_documents({"id": study["id"]}) == 0
        assert biosample_set.count_documents({"id": biosample["id"]}) == 0
        assert data_generation_set.count_documents({"id": data_generation["id"]}) == 0
        assert (
            data_object_set.count_documents(
                {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
            )
            == 0
        )
        assert (
            workflow_execution_set.count_documents({"id": workflow_execution["id"]})
            == 0
        )

        study_set.insert_many([study])
        biosample_set.insert_many([biosample])
        data_generation_set.insert_many([data_generation])
        data_object_set.insert_many([data_object_a, data_object_b])
        workflow_execution_set.insert_many([workflow_execution])

        # Update the `alldocs` collection, which is a cache used by the endpoint under test.
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

        yield mdb

        # 🧹 Clean up the source of truth collections (and then re-sync the `alldocs` collection).
        study_set.delete_many({"id": study["id"]})
        biosample_set.delete_many({"id": biosample["id"]})
        data_generation_set.delete_many({"id": data_generation["id"]})
        data_object_set.delete_many(
            {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
        )
        workflow_execution_set.delete_many({"id": workflow_execution["id"]})
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    def test_returns_404_error_for_nonexistent_study(self, api_site_client):
        r"""
        Confirms the endpoint returns an unsuccessful status code when no `Study` having the specified `id` exists.
        Reference: https://docs.pytest.org/en/stable/reference/reference.html#pytest.raises

        Note: The `api_site_client` fixture's `request` method will raise an exception if the server responds with
            an unsuccessful status code.
        """
        ensure_alldocs_collection_has_been_materialized()
        mdb = get_mongo_db()
        study_set = mdb.get_collection(name="study_set")
        nonexistent_study_id = "nmdc:sty-11-fake"
        assert study_set.count_documents({"id": nonexistent_study_id}) == 0
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            api_site_client.request(
                "GET",
                f"/data_objects/study/{nonexistent_study_id}",
            )
        assert exc_info.value.response.status_code == status.HTTP_404_NOT_FOUND

    def test_it_returns_empty_list_for_study_having_no_data_objects(
        self, api_site_client
    ):
        # Seed the test database with a study having no associated data objects.
        mdb = get_mongo_db()
        study_set = mdb.get_collection(name="study_set")
        alldocs = mdb.get_collection(name="alldocs")
        faker = Faker()
        study = faker.generate_studies(quantity=1)[0]
        assert study_set.count_documents({"id": study["id"]}) == 0
        study_set.insert_many([study])

        # Update the `alldocs` collection, which is a cache used by the endpoint under test.
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

        # Confirm the endpoint responds with no data objects.
        response = api_site_client.request("GET", f"/data_objects/study/{study['id']}")
        assert response.status_code == 200
        data_objects_by_biosample = response.json()
        assert len(data_objects_by_biosample) == 0

        # Clean up: Delete the documents we created within this test, from the database.
        study_set.delete_many({"id": study["id"]})
        alldocs.delete_many({})

    def test_it_returns_one_data_object_for_study_having_one(
        self, api_site_client, seeded_db
    ):
        # Dissociate all but one of the data objects from the workflow execution.
        workflow_execution_set = seeded_db.get_collection(name="workflow_execution_set")
        workflow_execution_set.update_one(
            {"id": self.workflow_execution_ids[0]},
            {"$set": {"has_output": [self.data_object_ids[0]]}},
        )

        # Update the `alldocs` collection, which is a cache used by the endpoint under test.
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

        # Confirm the endpoint responds with the data object we expect.
        response = api_site_client.request(
            "GET", f"/data_objects/study/{self.study_id}"
        )
        assert response.status_code == 200
        data_objects_by_biosample = response.json()
        assert len(data_objects_by_biosample) == 1
        received_biosample = data_objects_by_biosample[0]
        assert received_biosample["biosample_id"] == self.biosample_id
        assert len(received_biosample["data_objects"]) == 1
        received_data_object = received_biosample["data_objects"][0]
        assert received_data_object["id"] == self.data_object_ids[0]

    def test_it_returns_data_objects_for_study_having_multiple(
        self, api_site_client, seeded_db
    ):
        # Confirm the endpoint responds with the data objects we expect.
        response = api_site_client.request(
            "GET", f"/data_objects/study/{self.study_id}"
        )
        assert response.status_code == 200
        data_objects_by_biosample = response.json()
        assert len(data_objects_by_biosample) == 1
        received_biosample = data_objects_by_biosample[0]
        assert received_biosample["biosample_id"] == self.biosample_id
        assert len(received_biosample["data_objects"]) == 2
        received_data_objects = received_biosample["data_objects"]
        assert set(
            [
                self.data_object_ids[0],
                self.data_object_ids[1],
            ]
        ) == set([dobj["id"] for dobj in received_data_objects])

    @pytest.fixture()
    def seeded_db_with_multi_stage_wfe(self, seeded_db):
        r"""
        Fixture that seeds the database with a second `WorkflowExecution`, which takes the output
        of the first `WorkflowExecution` as its input, and outputs a new `DataObject`.

        ```mermaid
        graph
            workflow_execution_b --> |has_input| (existing data object)
            workflow_execution_b --> |has_output| data_object_c
        ```
        """
        faker = Faker()
        data_object_c = faker.generate_data_objects(
            quantity=1, id=self.data_object_ids[2]
        )[0]
        workflow_execution_b = faker.generate_metagenome_annotations(
            quantity=1,
            id=self.workflow_execution_ids[1],
            has_input=[
                self.data_object_ids[0]
            ],  # the output of the first `WorkflowExecution`
            has_output=[data_object_c["id"]],  # the new `DataObject`
            was_informed_by=self.data_generation_id,
        )[0]
        workflow_execution_set = seeded_db.get_collection(name="workflow_execution_set")
        assert (
            workflow_execution_set.count_documents({"id": workflow_execution_b["id"]})
            == 0
        )
        workflow_execution_set.insert_many([workflow_execution_b])
        data_object_set = seeded_db.get_collection(name="data_object_set")
        assert data_object_set.count_documents({"id": data_object_c["id"]}) == 0
        data_object_set.insert_many([data_object_c])

        # Update the `alldocs` collection, which is a cache used by the endpoint under test.
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

        yield seeded_db

        # Clean up: Delete the documents we created within this fixture, from the database.
        workflow_execution_set.delete_many({"id": workflow_execution_b["id"]})
        data_object_set.delete_many({"id": data_object_c["id"]})
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    def test_it_traverses_multiple_stages_of_workflow_executions(
        self, api_site_client, seeded_db_with_multi_stage_wfe
    ):
        # Confirm the database is seeded the way we expect.
        db = seeded_db_with_multi_stage_wfe  # concise alias
        workflow_execution_set = db.get_collection(name="workflow_execution_set")
        data_object_set = db.get_collection(name="data_object_set")
        assert (
            workflow_execution_set.count_documents(
                {"id": self.workflow_execution_ids[0]}
            )
            == 1
        )
        assert (
            workflow_execution_set.count_documents(
                {"id": self.workflow_execution_ids[1]}
            )
            == 1
        )
        assert data_object_set.count_documents({"id": self.data_object_ids[0]}) == 1
        assert data_object_set.count_documents({"id": self.data_object_ids[1]}) == 1
        assert data_object_set.count_documents({"id": self.data_object_ids[2]}) == 1

        # Confirm the endpoint responds with the data objects we expect.
        response = api_site_client.request(
            "GET", f"/data_objects/study/{self.study_id}"
        )
        assert response.status_code == 200
        data_objects_by_biosample = response.json()
        assert len(data_objects_by_biosample) == 1
        received_biosample = data_objects_by_biosample[0]
        assert received_biosample["biosample_id"] == self.biosample_id
        assert len(received_biosample["data_objects"]) == 3
        received_data_objects = received_biosample["data_objects"]
        received_data_object_ids = [dobj["id"] for dobj in received_data_objects]
        assert self.data_object_ids[0] in received_data_object_ids
        assert self.data_object_ids[1] in received_data_object_ids
        assert self.data_object_ids[2] in received_data_object_ids

    @pytest.fixture()
    def seeded_db_with_data_object_chain(self, seeded_db):
        r"""
        Fixture that seeds the database with a chain where a Biosample is fed as input
        (`has_input` slot) to a NucleotideSequencing process, which produces a DataObject
        as output (`has_output` slot). Then this DataObject is used as input into a
        MetagenomeAnnotation workflow, which produces another DataObject as output.

        ```mermaid
        graph
            biosample --> |has_input| nucleotide_sequencing_process
            nucleotide_sequencing_process --> |has_output| ntseq_dobj
            ntseq_dobj --> |has_input| metagenome_annotation_workflow
            metagenome_annotation_workflow --> |has_output| wfmgan_dobj
        ```
        """
        faker = Faker()

        # Create a NucleotideSequencing DataGeneration that produces a raw data object
        nucleotide_sequencing_id = "nmdc:ntseq-00-000001"
        ntseq_dobj = faker.generate_data_objects(quantity=1, id="nmdc:dobj-00-000004")[
            0
        ]
        ntseq_dobj["data_category"] = "instrument_data"

        nucleotide_sequencing_process = faker.generate_nucleotide_sequencings(
            quantity=1,
            id=nucleotide_sequencing_id,
            has_input=[self.biosample_id],
            has_output=[ntseq_dobj["id"]],
            associated_studies=[self.study_id],
        )[0]

        # Create a MetagenomeAnnotation workflow that takes the
        # NucleotideSequencing DataObject as input
        wfmgan_dobj = faker.generate_data_objects(quantity=1, id="nmdc:dobj-00-000005")[
            0
        ]
        wfmgan_dobj["data_category"] = "processed_data"

        metagenome_annotation_workflow = faker.generate_metagenome_annotations(
            quantity=1,
            has_input=[ntseq_dobj["id"]],
            was_informed_by=nucleotide_sequencing_id,
            id="nmdc:wfmgan-00-000001.1",
            has_output=[wfmgan_dobj["id"]],
        )[0]

        data_generation_set = seeded_db.get_collection(name="data_generation_set")
        workflow_execution_set = seeded_db.get_collection(name="workflow_execution_set")
        data_object_set = seeded_db.get_collection(name="data_object_set")

        data_generation_set.insert_many([nucleotide_sequencing_process])
        workflow_execution_set.insert_many([metagenome_annotation_workflow])
        data_object_set.insert_many([ntseq_dobj, wfmgan_dobj])

        # Update the `alldocs` collection, which is a cache used by the endpoint under test.
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

        yield seeded_db

        # Clean up: Delete the documents we created within this fixture, from the database.
        data_generation_set.delete_many({"id": nucleotide_sequencing_process["id"]})
        workflow_execution_set.delete_many({"id": metagenome_annotation_workflow["id"]})
        data_object_set.delete_many(
            {"id": {"$in": [ntseq_dobj["id"], wfmgan_dobj["id"]]}}
        )
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    def test_it_finds_data_objects_in_chain_where_data_objects_are_inputs(
        self, api_site_client, seeded_db_with_data_object_chain
    ):
        r"""
        Test that the endpoint finds DataObjects that are part of a chain where DataObjects
        serve as input to other processes (WorkflowExecution processes), validating functionality
        that allows DataObjects to be connected through has_input/has_output relationships.
        """
        # Access the fixture so my IDE doesn't warn about unused function parameters
        _ = seeded_db_with_data_object_chain

        # Confirm the endpoint responds with all data objects in the chain
        response = api_site_client.request(
            "GET", f"/data_objects/study/{self.study_id}"
        )
        assert response.status_code == 200
        data_objects_by_biosample = response.json()
        assert len(data_objects_by_biosample) == 1
        received_biosample = data_objects_by_biosample[0]
        assert received_biosample["biosample_id"] == self.biosample_id

        # Should find all 4 data objects: original 2, plus 2 from the nucleotide sequencing -> nom analysis chain
        assert len(received_biosample["data_objects"]) == 4
        received_data_objects = received_biosample["data_objects"]
        received_data_object_ids = [dobj["id"] for dobj in received_data_objects]

        # Verify all expected data objects are present
        expected_ids = [
            self.data_object_ids[0],  # original data_object_a
            self.data_object_ids[1],  # original data_object_b
            "nmdc:dobj-00-000004",  # data_object from nucleotide sequencing process
            "nmdc:dobj-00-000005",  # data_object from metagenome annotation workflow
        ]
        for expected_id in expected_ids:
            assert expected_id in received_data_object_ids

    @pytest.fixture()
    def seeded_db_with_informed_by_workflow(self, seeded_db):
        r"""
        Fixture that seeds the database with a chain where a Biosample is fed as input
        (`has_input` slot) to a NucleotideSequencing process, which produces a DataObject
        as output (`has_output` slot). Similar to the way in which the database is seeded above,
        the DataObject is used as input into a  MetagenomeAnnotation workflow, which produces
        another DataObject as output. In addition, the NucleotideSequencing process is also
        linked to the MetagenomeAnnotation workflow via the `was_informed_by` slot.

        ```mermaid
        graph
            biosample --> |has_input| nucleotide_sequencing_process
            nucleotide_sequencing_process --> |has_output| data_object_a
            data_object_a --> |has_input| metagenome_annotation_workflow
            metagenome_annotation_workflow --> |has_output| data_object_b
            metagenome_annotation_workflow --> |was_informed_by| nucleotide_sequencing_process
        ```
        """
        faker = Faker()

        # Create a NucleotideSequencing DataGeneration that produces raw data
        nucleotide_sequencing_b_id = "nmdc:ntseq-00-000002"
        data_object_a = faker.generate_data_objects(
            quantity=1, id="nmdc:dobj-00-000006"
        )[0]
        data_object_a["data_category"] = "instrument_data"

        nucleotide_sequencing_b = faker.generate_nucleotide_sequencings(
            quantity=1,
            id=nucleotide_sequencing_b_id,
            has_input=[self.biosample_id],
            has_output=[data_object_a["id"]],
            associated_studies=[self.study_id],
        )[0]

        # Create processed data object
        data_object_b = faker.generate_data_objects(
            quantity=1, id="nmdc:dobj-00-000007"
        )[0]
        data_object_b["data_category"] = "processed_data"

        # Create MetagenomeAnnotation workflow informed by the NucleotideSequencing
        metagenome_annotation_b = faker.generate_metagenome_annotations(
            quantity=1,
            has_input=[data_object_a["id"]],
            was_informed_by=nucleotide_sequencing_b_id,
            id="nmdc:wfmgan-00-000002.1",
            has_output=[data_object_b["id"]],
        )[0]

        data_generation_set = seeded_db.get_collection(name="data_generation_set")
        workflow_execution_set = seeded_db.get_collection(name="workflow_execution_set")
        data_object_set = seeded_db.get_collection(name="data_object_set")

        data_generation_set.insert_many([nucleotide_sequencing_b])
        workflow_execution_set.insert_many([metagenome_annotation_b])
        data_object_set.insert_many([data_object_a, data_object_b])

        # Update the `alldocs` collection, which is a cache used by the endpoint under test.
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

        yield seeded_db

        # Clean up: Delete the documents we created within this fixture, from the database.
        data_generation_set.delete_many({"id": nucleotide_sequencing_b["id"]})
        workflow_execution_set.delete_many({"id": metagenome_annotation_b["id"]})
        data_object_set.delete_many(
            {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
        )
        ensure_alldocs_collection_has_been_materialized(force_refresh_of_alldocs=True)

    def test_it_finds_data_objects_via_informed_by_workflow_linking(
        self, api_site_client, seeded_db_with_informed_by_workflow
    ):
        r"""
        Test that the endpoint finds DataObjects through WorkflowExecutions that are
        linked to DataGeneration records via the `was_informed_by` slot, validating
        functionality that processes DataGeneration descendants.
        """
        # Access the fixture so my IDE doesn't warn about unused function parameters
        _ = seeded_db_with_informed_by_workflow

        # Confirm the endpoint responds with data objects found via informed_by linking
        response = api_site_client.request(
            "GET", f"/data_objects/study/{self.study_id}"
        )
        assert response.status_code == 200
        data_objects_by_biosample = response.json()
        assert len(data_objects_by_biosample) == 1
        received_biosample = data_objects_by_biosample[0]
        assert received_biosample["biosample_id"] == self.biosample_id

        # Should find original 2 data objects plus 2 from informed_by workflow chain
        assert len(received_biosample["data_objects"]) == 4
        received_data_objects = received_biosample["data_objects"]
        received_data_object_ids = [dobj["id"] for dobj in received_data_objects]

        # Verify all expected data objects are present
        expected_ids = [
            self.data_object_ids[0],  # original data_object_a
            self.data_object_ids[1],  # original data_object_b
            "nmdc:dobj-00-000006",  # data_object_b from nucleotide sequencing
            "nmdc:dobj-00-000007",  # data_object_b from nom analysis
        ]
        for expected_id in expected_ids:
            assert expected_id in received_data_object_ids


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
    try:
        assert rv.json()["meta"]["count"] >= 9
    finally:  # clean up
        for collection_name, docs in database_dict.items():
            mdb[collection_name].delete_one(
                {"id": {"$in": [doc["id"] for doc in docs]}}
            )


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

    # clean up
    for collection_name, docs in database_dict.items():
        mdb[collection_name].delete_one({"id": {"$in": [doc["id"] for doc in docs]}})


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
        mdb.biosample_set.insert_one({"id": biosample_id, "type": "nmdc:Biosample"})

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


def test_queries_run_rejects_deletions_that_would_leave_broken_references(
    api_user_client,
    fake_studies_and_biosamples_in_mdb,
):
    study_a, study_b, bsm_a, bsm_b = fake_studies_and_biosamples_in_mdb

    # Ensure the user has permission to issue "delete" commands via the `/queries:run` API endpoint.
    mdb = get_mongo_db()
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)

    # Case 1: We cannot delete Study A because Biosample A and Study B are referencing it.
    # Reference: https://docs.pytest.org/en/6.2.x/reference.html#pytest-raises
    with pytest.raises(requests.HTTPError) as exc_info:
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "delete": "study_set",
                "deletes": [
                    {
                        "q": {"id": study_a["id"]},
                        "limit": 0,
                    }
                ],
            },
        )
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Case 2: We cannot delete Study B because Biosample B is referencing it.
    with pytest.raises(requests.HTTPError) as exc_info:
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "delete": "study_set",
                "deletes": [
                    {
                        "q": {"id": study_b["id"]},
                        "limit": 0,
                    }
                ],
            },
        )
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Case 3: We can delete both Biosample A and Biosample B because nothing is referencing them.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "delete": "biosample_set",
            "deletes": [
                {
                    "q": {"id": {"$in": [bsm_a["id"], bsm_b["id"]]}},
                    "limit": 0,
                }
            ],
        },
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["n"] == 2

    # Case 4: Now, we _can_ delete both Study A and Study B because, although Study A is still
    #         referenced by Study B, deleting them both as part of the same operation will not
    #         _leave behind_ any broken references.
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "delete": "study_set",
            "deletes": [
                {
                    "q": {"id": {"$in": [study_a["id"], study_b["id"]]}},
                    "limit": 0,
                }
            ],
        },
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["n"] == 2


def test_queries_run_allows_ref_breaking_deletions_when_user_opts_to_allow_broken_refs(
    api_user_client,
    fake_studies_and_biosamples_in_mdb,
):
    study_a, _, _, _ = fake_studies_and_biosamples_in_mdb

    # Ensure the user has permission to issue "delete" commands via the `/queries:run` API endpoint.
    mdb = get_mongo_db()
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)

    # Case 1: We cannot delete Study A because Biosample A and Study B are referencing it.
    # Reference: https://docs.pytest.org/en/6.2.x/reference.html#pytest-raises
    with pytest.raises(requests.HTTPError) as exc_info:
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "delete": "study_set",
                "deletes": [
                    {
                        "q": {"id": study_a["id"]},
                        "limit": 0,
                    }
                ],
            },
        )
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Case 2: We can do it if we opt out of the referential integrity checks.
    response = api_user_client.request(
        "POST",
        "/queries:run?allow_broken_refs=true",
        {
            "delete": "study_set",
            "deletes": [
                {
                    "q": {"id": study_a["id"]},
                    "limit": 0,
                }
            ],
        },
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["n"] == 1


def test_queries_run_rejects_updates_that_would_leave_broken_references(
    api_user_client,
    fake_studies_and_biosamples_in_mdb,
):
    r"""
    This test focuses on the general behavior of the API _endpoint_ when the update
    would leave behind broken references. We have a different set of tests, in
    `tests/test_api/test_endpoints_lib.py`, focused on specific scenarios.
    """
    study_a, _, _, _ = fake_studies_and_biosamples_in_mdb

    # Ensure the user has permission to issue "update" commands via the `/queries:run` API endpoint.
    # Note: The same "allowance" document is used for both "update" and "delete" commands.
    mdb = get_mongo_db()
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)

    # Confirm the endpoint doesn't allow us to introduce a reference to a nonexistent document.
    nonexistent_study_id = "nmdc:sty-00-000099"
    assert mdb.study_set.count_documents({"id": nonexistent_study_id}) == 0
    with pytest.raises(requests.HTTPError) as exc_info:
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "update": "study_set",
                "updates": [
                    {
                        "q": {"id": study_a["id"]},
                        "u": {"$set": {"part_of": [nonexistent_study_id]}},
                    }
                ],
            },
        )
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    response_body = exc_info.value.response.json()
    assert "study_set" in response_body["detail"]


def test_queries_run_allows_ref_breaking_updates_when_user_opts_to_allow_broken_refs(
    api_user_client,
    fake_studies_and_biosamples_in_mdb,
):
    study_a, _, _, _ = fake_studies_and_biosamples_in_mdb

    # Ensure the user has permission to issue "update" commands via the `/queries:run` API endpoint.
    # Note: The same "allowance" document is used for both "update" and "delete" commands.
    mdb = get_mongo_db()
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)

    # Confirm the endpoint doesn't allow us to introduce a reference to a nonexistent document.
    nonexistent_study_id = "nmdc:sty-00-000099"
    assert mdb.study_set.count_documents({"id": nonexistent_study_id}) == 0
    with pytest.raises(requests.HTTPError) as exc_info:
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "update": "study_set",
                "updates": [
                    {
                        "q": {"id": study_a["id"]},
                        "u": {"$set": {"part_of": [nonexistent_study_id]}},
                    }
                ],
            },
        )
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    response_body = exc_info.value.response.json()
    assert "study_set" in response_body["detail"]

    # Now, confirm the endpoint allows us to introduce a reference to a nonexistent document
    # if we tell it to allow broken references.
    response = api_user_client.request(
        "POST",
        "/queries:run?allow_broken_refs=true",
        {
            "update": "study_set",
            "updates": [
                {
                    "q": {"id": study_a["id"]},
                    "u": {"$set": {"part_of": [nonexistent_study_id]}},
                }
            ],
        },
    )
    assert response.status_code == status.HTTP_200_OK


def test_find_related_resources_for_workflow_execution__returns_404_if_wfe_nonexistent(
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


def test_find_related_resources_for_workflow_execution__returns_related_resources(
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
    assert response_payload["workflow_execution"] == strip_oid(workflow_execution)
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


def test_find_related_resources_for_workflow_execution__returns_related_workflow_executions(
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
        1, was_generated_by=data_generation_a["id"]
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
    assert response_payload["workflow_execution"] == strip_oid(workflow_execution_a)
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
