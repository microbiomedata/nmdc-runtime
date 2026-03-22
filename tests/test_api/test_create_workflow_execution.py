import pytest
import requests
from starlette import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from tests.lib.faker import Faker


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
        was_informed_by=[data_generation["id"]],
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
        was_informed_by=[
            nonexistent_data_generation_id
        ],  # intentionally-broken reference
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