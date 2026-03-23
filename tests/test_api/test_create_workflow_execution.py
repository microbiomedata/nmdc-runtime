import re
from typing import List

import pytest
import requests
from fastapi import status
from pymongo.collection import Collection

from nmdc_runtime.api.db.mongo import get_mongo_db
from tests.lib.faker import Faker


def generate_available_id_for_collection(
    collection: Collection,
    prefix: str,
    suffix: str = "",
    disallowed_ids: List[str] = list(),
) -> str:
    """
    Generates an ID beginning with the specified prefix (and optional suffix), that is not already
    in use by any documents in the specified Mongo collection and is not among the disallowed IDs.
    The suffix can be used to append a ".1" to a `WorkflowExecution` identifier, given that NMDC
    workflow automation team members normally append integers to minted `WorkflowExecution`
    identifiers. The `disallowed_ids` parameter can be used to specify a list of IDs that you
    have already generated, but haven't inserted into the database yet.
    """
    n = 1
    available_id = f"{prefix}-00-{n:06d}{suffix}"  # e.g. "nmdc:wfmgan-00-000001"
    while available_id in disallowed_ids or collection.count_documents({"id": available_id}, limit=1) != 0:
        n += 1
        available_id = f"{prefix}-00-{n:06d}{suffix}"  # e.g. "nmdc:wfmgan-00-000002"
    return available_id


class TestPostWorkflowWorkflowExecutions:
    """Tests targeting the `POST /workflows/workflow_executions` API endpoint."""

    @pytest.fixture
    def seeded_db_having_workflow_execution_dependencies(self):
        """
        Yields (a) a database that has been seeded with documents that a `WorkflowExecution` can
        reference; (b) references to those documents; and (c) an available `WorkflowExecution` ID.
        After execution returns to this fixture, the fixture deletes the seeded documents and any
        `WorkflowExecution` having that previously-available ID.
        """

        # Generate some documents.
        faker = Faker()
        study = faker.generate_studies(quantity=1)[0]
        biosample = faker.generate_biosamples(quantity=1, associated_studies=[study["id"]])[0]
        data_object_a, data_object_b, data_object_c, data_object_d = faker.generate_data_objects(quantity=4)
        data_generation = faker.generate_nucleotide_sequencings(
            quantity=1, associated_studies=[study["id"]], has_input=[biosample["id"]]
        )[0]

        # Get references to relevant Mongo collections.
        db = get_mongo_db()
        study_set = db.get_collection("study_set")
        biosample_set = db.get_collection("biosample_set")
        data_object_set = db.get_collection("data_object_set")
        data_generation_set = db.get_collection("data_generation_set")
        workflow_execution_set = db.get_collection("workflow_execution_set")

        # Confirm that the generated documents are not already in the test database.
        assert study_set.count_documents({"id": study["id"]}) == 0
        assert biosample_set.count_documents({"id": biosample["id"]}) == 0
        assert (
            data_object_set.count_documents(
                {"id": {"$in": [data_object_a["id"], data_object_b["id"], data_object_c["id"], data_object_d["id"]]}}
            )
            == 0
        )
        assert data_generation_set.count_documents({"id": data_generation["id"]}) == 0

        # Insert the generated documents.
        study_set.insert_one(study)
        biosample_set.insert_one(biosample)
        data_object_set.insert_many([data_object_a, data_object_b, data_object_c, data_object_d])
        data_generation_set.insert_one(data_generation)

        # Make up a `WorkflowExecution` ID and confirm it's not in use.
        available_workflow_execution_id = generate_available_id_for_collection(
            workflow_execution_set, "nmdc:wfmgan", ".1",
        )
        assert workflow_execution_set.count_documents({"id": available_workflow_execution_id}) == 0

        # Make up a `DataGeneration` ID and confirm it's not in use.
        available_data_generation_id = generate_available_id_for_collection(
            data_generation_set, "nmdc:dgns",
        )
        assert data_generation_set.count_documents({"id": available_data_generation_id}) == 0

        # Yield the database, some of the seeded documents, and some available IDs.
        yield (
            db,
            # seeded data
            {
                "data_object_a": data_object_a,
                "data_object_b": data_object_b,
                "data_object_c": data_object_c,
                "data_object_d": data_object_d,
                "data_generation": data_generation,
            },
            # available IDs
            {
                "workflow_execution_id": available_workflow_execution_id,
                "data_generation_id": available_data_generation_id,
            }
        )

        # Delete the documents that we created or that the dependent test created.
        study_set.delete_many({"id": study["id"]})
        biosample_set.delete_many({"id": biosample["id"]})
        data_object_set.delete_many(
            {"id": {"$in": [data_object_a["id"], data_object_b["id"], data_object_c["id"], data_object_d["id"]]}}
        )
        data_generation_set.delete_many({"id": data_generation["id"]})

        # Delete the `WorkflowExecution` having the previously-available ID.
        workflow_execution_set.delete_many({"id": available_workflow_execution_id})

    @pytest.fixture
    def seeded_db_having_workflow_execution(self, seeded_db_having_workflow_execution_dependencies):
        """Seeds the database with a `WorkflowExecution` that references the seeded documents from the
        `seeded_db_having_workflow_execution_dependencies` fixture, then deletes that `WorkflowExecution`
        after the test.
        """
        # Get references to the database, relevant seeded data, and available IDs.
        db, seeded_data, available_ids = seeded_db_having_workflow_execution_dependencies
        data_object_a = seeded_data["data_object_a"]
        data_object_b = seeded_data["data_object_b"]
        data_generation = seeded_data["data_generation"]
        workflow_execution_id = available_ids["workflow_execution_id"]

        # Generate a `WorkflowExecution` document that references the seeded documents, then insert
        # it into the database.
        faker = Faker()
        workflow_execution = faker.generate_metagenome_annotations(
            quantity=1,
            id=workflow_execution_id,
            has_input=[data_object_a["id"]],
            has_output=[data_object_b["id"]],
            was_informed_by=[data_generation["id"]],
        )[0]
        workflow_execution_set = db.get_collection("workflow_execution_set")
        workflow_execution_set.insert_one(workflow_execution)

        # Yield the database, some of the seeded documents, and some available IDs.
        yield (
            db,
            # seeded data
            {
                "data_object_a": data_object_a,
                "data_object_b": data_object_b,
                "data_object_c": seeded_data["data_object_c"],
                "data_object_d": seeded_data["data_object_d"],
                "data_generation": data_generation,
                "workflow_execution": workflow_execution,
            },
            # available IDs
            {}
        )

         # Delete the `WorkflowExecution` that we created.
        workflow_execution_set.delete_many({"id": workflow_execution_id})

    def test_it_inserts_workflow_execution(
        self,
        api_site_client,
        seeded_db_having_workflow_execution_dependencies,
    ):
        """Submit a valid WFE to the API endpoint, then confirm it exists in the database."""

        # Get references to the database, relevant seeded data, and available IDs.
        db, seeded_data, available_ids = seeded_db_having_workflow_execution_dependencies
        data_object_a = seeded_data["data_object_a"]
        data_object_b = seeded_data["data_object_b"]
        data_generation = seeded_data["data_generation"]
        workflow_execution_id = available_ids["workflow_execution_id"]
        workflow_execution_set = db.get_collection("workflow_execution_set")

        try:
            # Confirm the document we're about to create does not exist in the database yet.
            assert workflow_execution_set.count_documents({"id": workflow_execution_id}) == 0

            # Generate a `WorkflowExecution` dictionary for the API request payload.
            faker = Faker()
            workflow_execution = faker.generate_metagenome_annotations(
                quantity=1,
                id=workflow_execution_id,
                has_input=[data_object_a["id"]],
                has_output=[data_object_b["id"]],
                was_informed_by=[data_generation["id"]],
            )[0]

            # Submit an API request whose payload contains the `WorkflowExecution` document.
            response = api_site_client.request(
                "POST",
                "/workflows/workflow_executions",
                {"workflow_execution_set": [workflow_execution]},
            )
            assert response.status_code == 200
            response_message = response.json()["message"]
            assert re.search(r"^Inserted \d+ documents$", response_message) is not None

            # Assert that the `workflow_execution_set` collection now contains the document we submitted.
            assert workflow_execution_set.count_documents({"id": workflow_execution_id}) == 1
        finally:
            # Delete the `WorkflowExecution` that we created.
            workflow_execution_set.delete_many({"id": workflow_execution_id})


    def test_it_rejects_workflow_execution_containing_broken_reference(
        self,
        api_site_client,
        seeded_db_having_workflow_execution_dependencies,
    ):
        """
        Submit a `WorkflowExecution` that contains a reference to a non-existent `DataGeneration`,
        then confirm the endpoint responds with an HTTP 422 status.
        """

        # Get references to the database, relevant seeded data, and available IDs.
        db, seeded_data, available_ids = seeded_db_having_workflow_execution_dependencies
        data_object_a = seeded_data["data_object_a"]
        data_object_b = seeded_data["data_object_b"]
        workflow_execution_id = available_ids["workflow_execution_id"]
        data_generation_id = available_ids["data_generation_id"]
        workflow_execution_set = db.get_collection("workflow_execution_set")

        try:
            # Confirm the document we're about to create does not exist in the database yet.
            assert workflow_execution_set.count_documents({"id": workflow_execution_id}) == 0

            # Generate a `WorkflowExecution` dictionary for the API request payload.
            faker = Faker()
            workflow_execution = faker.generate_metagenome_annotations(
                quantity=1,
                id=workflow_execution_id,
                has_input=[data_object_a["id"]],
                has_output=[data_object_b["id"]],
                was_informed_by=[data_generation_id],
            )[0]

            # Submit an API request whose payload contains the `WorkflowExecution` document, which
            # contains a (broken) reference to a non-existent `DataGeneration`.
            with pytest.raises(requests.exceptions.HTTPError) as exc:
                api_site_client.request(
                    "POST",
                    "/workflows/workflow_executions",
                    {"workflow_execution_set": [workflow_execution]},
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
            assert workflow_execution_set.count_documents({"id": workflow_execution_id}) == 0
        finally:
            pass  # nothing to clean up

    def test_it_inserts_workflow_executions_and_data_objects(
        self,
        api_site_client,
        seeded_db_having_workflow_execution_dependencies,
    ):
        """
        Submit a `WorkflowExecution` along with some `DataObject`s that it references, then confirm
        the endpoint inserts all of those documents.
        """

        # Get references to the database, relevant seeded data, and available IDs.
        db, seeded_data, available_ids = seeded_db_having_workflow_execution_dependencies
        data_object_set = db.get_collection("data_object_set")
        workflow_execution_set = db.get_collection("workflow_execution_set")
        data_generation = seeded_data["data_generation"]
        workflow_execution_id = available_ids["workflow_execution_id"]

        # Generate a `WorkflowExecution` and its `DataObject`s and insert them into the database.
        faker = Faker()
        data_object_e, data_object_f = faker.generate_data_objects(quantity=2)
        data_object_e["id"] = generate_available_id_for_collection(data_object_set, "nmdc:dobj")
        data_object_f["id"] = generate_available_id_for_collection(data_object_set, "nmdc:dobj", disallowed_ids=[data_object_e["id"]])
        workflow_execution = faker.generate_metagenome_annotations(
            quantity=1,
            id=workflow_execution_id,
            has_input=[data_object_e["id"]],
            has_output=[data_object_f["id"]],
            was_informed_by=[data_generation["id"]],
        )[0]

        try:
            # Confirm the documents we're about to create do not exist in the database yet.
            assert workflow_execution_set.count_documents({"id": workflow_execution_id}) == 0
            assert data_object_set.count_documents({"id": data_object_e["id"]}) == 0
            assert data_object_set.count_documents({"id": data_object_f["id"]}) == 0

            # Submit an API request whose payload contains the `WorkflowExecution` document and its
            # referenced `DataObject` documents.
            response = api_site_client.request(
                "POST",
                "/workflows/workflow_executions",
                {
                    "workflow_execution_set": [workflow_execution],
                    "data_object_set": [data_object_e, data_object_f],
                },
            )
            assert response.status_code == status.HTTP_200_OK
            response_message = response.json()["message"]
            assert re.search(r"^Inserted 3 documents$", response_message) is not None

            # Assert that the database now contains the documents we submitted.
            assert workflow_execution_set.count_documents({"id": workflow_execution_id}) == 1
            assert data_object_set.count_documents({"id": data_object_e["id"]}) == 1
            assert data_object_set.count_documents({"id": data_object_f["id"]}) == 1
        finally:
            # Delete the documents that we created.
            workflow_execution_set.delete_many({"id": workflow_execution_id})
            data_object_set.delete_many({"id": {"$in": [data_object_e["id"], data_object_f["id"]]}})

    def test_it_updates_superseded_by_fields_of_existing_workflow_executions_and_data_objects(
        self,
        api_site_client,
        seeded_db_having_workflow_execution,
    ):
        """
        Given a database containing an "existing" WFE and its DOBJs.
        Submit a WFE whose `id` value indicates that it supersedes the seeded WFE.
        Confirm the `superseded_by` fields of the "existing" WFE and its output DOBJs reference the new one.
        """
        db, seeded_data, _ = seeded_db_having_workflow_execution
        workflow_execution_set = db.get_collection("workflow_execution_set")
        data_object_set = db.get_collection("data_object_set")

        faker = Faker()
        superseding_wfe = faker.generate_metagenome_annotations(
            quantity=1,
            id=generate_available_id_for_collection(workflow_execution_set, "nmdc:wfmgan", ".2"),
            has_input=[seeded_data["data_object_a"]["id"]],
            has_output=[seeded_data["data_object_b"]["id"]],  # <-- this is the output DOBJ
            was_informed_by=[seeded_data["data_generation"]["id"]],
        )[0]

        try:
            # Confirm the seeded WFE and its output DOBJs do not have `superseded_by` values yet.
            assert "superseded_by" not in seeded_data["workflow_execution"]
            assert "superseded_by" not in seeded_data["data_object_b"]

            # Submit an API request whose payload contains the superseding WFE.
            response = api_site_client.request(
                "POST",
                "/workflows/workflow_executions",
                {
                    "workflow_execution_set": [superseding_wfe],
                },
            )
            assert response.status_code == status.HTTP_200_OK
            response_message = response.json()["message"]
            assert re.search(r"^Inserted 1 documents$", response_message) is not None

            # Assert that the `superseded_by` fields of the seeded WFE and its DOBJs now reference the superseding WFE.
            seeded_wfe_in_db = workflow_execution_set.find_one({"id": seeded_data["workflow_execution"]["id"]})
            assert "superseded_by" in seeded_wfe_in_db
            assert seeded_wfe_in_db["superseded_by"] == superseding_wfe["id"]
            seeded_data_object_b_in_db = data_object_set.find_one({"id": seeded_data["data_object_b"]["id"]})
            assert "superseded_by" in seeded_data_object_b_in_db
            assert seeded_data_object_b_in_db["superseded_by"] == superseding_wfe["id"]

            # Assert that the other seeded DOBJs still have no `superseded_by` fields.
            seeded_data_object_a_in_db = data_object_set.find_one({"id": seeded_data["data_object_a"]["id"]})
            assert "superseded_by" not in seeded_data_object_a_in_db
            seeded_data_object_c_in_db = data_object_set.find_one({"id": seeded_data["data_object_c"]["id"]})
            assert "superseded_by" not in seeded_data_object_c_in_db
            seeded_data_object_d_in_db = data_object_set.find_one({"id": seeded_data["data_object_d"]["id"]})
            assert "superseded_by" not in seeded_data_object_d_in_db
        finally:
            # Delete the superseding WFE that we created.
            workflow_execution_set.delete_many({"id": superseding_wfe["id"]})
