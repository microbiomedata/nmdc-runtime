import pytest
import requests
from starlette import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from tests.lib.faker import Faker


@pytest.fixture
def seeded_valid_workflow_execution_data():
    """Seed referenced documents for a valid workflow execution request and clean up afterward."""

    faker = Faker()
    study = faker.generate_studies(quantity=1)[0]
    biosample = faker.generate_biosamples(quantity=1, associated_studies=[study["id"]])[0]
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
        was_informed_by=[data_generation["id"]],
    )[0]

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

    study_set.insert_one(study)
    biosample_set.insert_one(biosample)
    data_object_set.insert_many([data_object_a, data_object_b])
    data_generation_set.insert_one(data_generation)

    yield {
        "workflow_execution": workflow_execution,
        "workflow_execution_set": workflow_execution_set,
        "study": study,
        "biosample": biosample,
        "data_object_ids": [data_object_a["id"], data_object_b["id"]],
        "data_generation": data_generation,
        "study_set": study_set,
        "biosample_set": biosample_set,
        "data_object_set": data_object_set,
        "data_generation_set": data_generation_set,
    }

    study_set.delete_many({"id": study["id"]})
    biosample_set.delete_many({"id": biosample["id"]})
    data_object_set.delete_many(
        {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
    )
    data_generation_set.delete_many({"id": data_generation["id"]})
    workflow_execution_set.delete_many({"id": workflow_execution["id"]})


@pytest.fixture
def seeded_workflow_execution_with_broken_reference_data():
    """Seed only data objects for a broken-reference workflow execution request and clean up afterward."""

    faker = Faker()
    nonexistent_data_generation_id = "nmdc:dgns-00-000001"
    data_object_a, data_object_b = faker.generate_data_objects(quantity=2)
    workflow_execution = faker.generate_metagenome_annotations(
        quantity=1,
        has_input=[data_object_a["id"]],
        has_output=[
            data_object_b["id"]
        ],  # schema says field optional; but validator complains when absent
        was_informed_by=[
            nonexistent_data_generation_id
        ],  # intentionally-broken reference
    )[0]

    mdb = get_mongo_db()
    data_generation_set = mdb.get_collection("data_generation_set")
    data_object_set = mdb.get_collection("data_object_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")

    assert data_generation_set.count_documents({"id": nonexistent_data_generation_id}) == 0
    assert (
        data_object_set.count_documents(
            {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
        )
        == 0
    )
    assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 0

    data_object_set.insert_many([data_object_a, data_object_b])

    yield {
        "workflow_execution": workflow_execution,
        "workflow_execution_set": workflow_execution_set,
        "data_object_ids": [data_object_a["id"], data_object_b["id"]],
        "data_object_set": data_object_set,
    }

    data_object_set.delete_many(
        {"id": {"$in": [data_object_a["id"], data_object_b["id"]]}}
    )
    workflow_execution_set.delete_many({"id": workflow_execution["id"]})


def test_post_workflows_workflow_executions_inserts_submitted_document(
    api_site_client,
    seeded_valid_workflow_execution_data,
):
    r"""
    In this test, we submit a workflow execution to the `/workflows/workflow_executions` API endpoint,
    and then confirm that that workflow execution has been inserted into the database.
    """

    workflow_execution = seeded_valid_workflow_execution_data["workflow_execution"]
    workflow_execution_set = seeded_valid_workflow_execution_data["workflow_execution_set"]

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


def test_post_workflows_workflow_executions_rejects_document_containing_broken_reference(
    api_site_client,
    seeded_workflow_execution_with_broken_reference_data,
):
    r"""
    In this test, we submit a workflow execution that contains a reference to a non-existent data generation,
    to the `/workflows/workflow_executions` API endpoint, and confirm the endpoint returns an error response.
    """

    workflow_execution = seeded_workflow_execution_with_broken_reference_data[
        "workflow_execution"
    ]
    workflow_execution_set = seeded_workflow_execution_with_broken_reference_data[
        "workflow_execution_set"
    ]

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