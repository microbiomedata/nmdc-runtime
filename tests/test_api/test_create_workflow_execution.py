import re

import pytest
import requests
from fastapi import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.allowance import AllowanceAction
from tests.lib.faker import Faker


class TestPostWorkflowWorkflowExecutions:
    """Tests targeting the `POST /workflows/workflow_executions` API endpoint."""

    @pytest.fixture()
    def api_user_client_having_json_submit_allowance(self, api_user_client):
        """Yields an API user client for a user having a specific allowance."""

        mdb = get_mongo_db()

        allowance = {
            "username": api_user_client.username,
            "action": AllowanceAction.SUBMIT_JSON.value,
        }

        allowances_coll = mdb.get_collection("_runtime.api.allow")

        # Grant the allowance.
        allowances_coll.insert_one(allowance)

        yield api_user_client

        # Cleanup: Revoke the allowance.
        allowances_coll.delete_one(allowance)

    @pytest.fixture()
    def api_site_client_having_json_submit_allowance(self, api_site_client):
        """Yields an API user client for a user having a specific allowance."""

        mdb = get_mongo_db()

        allowance = {
            "username": api_site_client.client_id,
            "action": AllowanceAction.SUBMIT_JSON.value,
        }

        allowances_coll = mdb.get_collection("_runtime.api.allow")

        # Grant the allowance.
        allowances_coll.insert_one(allowance)

        yield api_site_client

        # Cleanup: Revoke the allowance.
        allowances_coll.delete_one(allowance)

    @pytest.fixture
    def seeded_db_having_workflow_execution_dependencies(self):
        """
        Yields (a) a database that has been seeded with documents that a `WorkflowExecution` can
        reference and (b) references to those documents. Deletes the seeded documents after
        resuming execution.
        """

        # Generate some documents.
        faker = Faker()
        study = faker.generate_studies(quantity=1)[0]
        biosample = faker.generate_biosamples(quantity=1, associated_studies=[study["id"]])[0]
        data_object_a, data_object_b, data_object_c, data_object_d = faker.generate_data_objects(quantity=4)
        data_generation = faker.generate_nucleotide_sequencings(quantity=1, associated_studies=[study["id"]], has_input=[biosample["id"]])[0]
        data_object_ids = [data_object_a["id"], data_object_b["id"], data_object_c["id"], data_object_d["id"]]

        # Get references to relevant Mongo collections.
        db = get_mongo_db()
        study_set = db.get_collection("study_set")
        biosample_set = db.get_collection("biosample_set")
        data_object_set = db.get_collection("data_object_set")
        data_generation_set = db.get_collection("data_generation_set")

        # Confirm that the generated documents are not already in the test database.
        assert study_set.count_documents({"id": study["id"]}) == 0
        assert biosample_set.count_documents({"id": biosample["id"]}) == 0
        assert data_object_set.count_documents({"id": {"$in": data_object_ids}}) == 0
        assert data_generation_set.count_documents({"id": data_generation["id"]}) == 0

        # Insert the generated documents.
        study_set.insert_one(study)
        biosample_set.insert_one(biosample)
        data_object_set.insert_many([data_object_a, data_object_b, data_object_c, data_object_d])
        data_generation_set.insert_one(data_generation)

        # Yield the database and seeded documents.
        yield (
            db,
            {
                "study": study,
                "biosample": biosample,
                "data_object_a": data_object_a,
                "data_object_b": data_object_b,
                "data_object_c": data_object_c,
                "data_object_d": data_object_d,
                "data_generation": data_generation,
            },
        )

        # Delete the documents that we created or that the dependent test created.
        study_set.delete_many({"id": study["id"]})
        biosample_set.delete_many({"id": biosample["id"]})
        data_object_set.delete_many({"id": {"$in": data_object_ids}})
        data_generation_set.delete_many({"id": data_generation["id"]})

    @pytest.fixture
    def seeded_db_having_workflow_execution(self, seeded_db_having_workflow_execution_dependencies):
        """
        Seeds the database with a `WorkflowExecution` that references the seeded documents from
        the other fixture. Deletes the additional seeded document after resuming execution.
        """
        # Get references to the database and relevant seeded data.
        db, seeded_data = seeded_db_having_workflow_execution_dependencies
        data_object_a = seeded_data["data_object_a"]
        data_object_b = seeded_data["data_object_b"]
        data_generation = seeded_data["data_generation"]
        workflow_execution_set = db.get_collection("workflow_execution_set")
        workflow_execution_id = "nmdc:wfmgan-00-000001.1"

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
        workflow_execution_set.insert_one(workflow_execution)

        # Yield the database and seeded documents.
        yield (
            db,
            {
                "data_object_a": data_object_a,
                "data_object_b": data_object_b,
                "data_object_c": seeded_data["data_object_c"],
                "data_object_d": seeded_data["data_object_d"],
                "data_generation": data_generation,
                "workflow_execution": workflow_execution,
                "workflow_execution_id": workflow_execution_id,
            }
        )

         # Delete the `WorkflowExecution` that we created.
        workflow_execution_set.delete_many({"id": workflow_execution_id})

    def test_it_forbids_unauthorized_users(self, api_user_client):
        """Confirm that an unauthorized user cannot access this endpoint."""

        with pytest.raises(requests.exceptions.HTTPError) as exc:
            api_user_client.request(
                "POST",
                "/workflows/workflow_executions",
                {"workflow_execution_set": []},
            )
        response = exc.value.response
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_it_forbids_unauthorized_site_clients(self, api_site_client):
        """Confirm that an unauthorized site client cannot access this endpoint."""

        with pytest.raises(requests.exceptions.HTTPError) as exc:
            api_site_client.request(
                "POST",
                "/workflows/workflow_executions",
                {"workflow_execution_set": []},
            )
        response = exc.value.response
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_it_allows_authorized_users(self, api_user_client_having_json_submit_allowance):
        """Confirm that an authorized user can access this endpoint."""

        response = api_user_client_having_json_submit_allowance.request(
            "POST",
            "/workflows/workflow_executions",
            {"workflow_execution_set": []},  # no-op, but OK
        )
        assert response.status_code == status.HTTP_200_OK

    def test_it_allows_authorized_site_clients(self, api_site_client_having_json_submit_allowance):
        """Confirm that an authorized site client can access this endpoint."""

        response = api_site_client_having_json_submit_allowance.request(
            "POST",
            "/workflows/workflow_executions",
            {"workflow_execution_set": []},  # no-op, but OK
        )
        assert response.status_code == status.HTTP_200_OK

    def test_it_inserts_workflow_execution(
        self,
        api_site_client_having_json_submit_allowance,
        seeded_db_having_workflow_execution_dependencies,
    ):
        """Submit a valid WFE to the API endpoint, then confirm it exists in the database."""

        # Get references to the database and relevant seeded data.
        db, seeded_data = seeded_db_having_workflow_execution_dependencies
        data_object_a = seeded_data["data_object_a"]
        data_object_b = seeded_data["data_object_b"]
        data_generation = seeded_data["data_generation"]
        workflow_execution_set = db.get_collection("workflow_execution_set")
        workflow_execution_id = "nmdc:wfmgan-00-000002.1"

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
            response = api_site_client_having_json_submit_allowance.request(
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
        api_site_client_having_json_submit_allowance,
        seeded_db_having_workflow_execution_dependencies,
    ):
        """
        Submit a `WorkflowExecution` that contains a reference to a non-existent `DataGeneration`,
        then confirm the endpoint responds with an HTTP 422 status due to the broken reference.
        """

        # Get references to the database and relevant seeded data.
        db, seeded_data = seeded_db_having_workflow_execution_dependencies
        data_object_a = seeded_data["data_object_a"]
        data_object_b = seeded_data["data_object_b"]
        workflow_execution_set = db.get_collection("workflow_execution_set")
        workflow_execution_id = "nmdc:wfmgan-00-000003.1"
        data_generation_id = "nmdc:dgns-00-000002"

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
                api_site_client_having_json_submit_allowance.request(
                    "POST",
                    "/workflows/workflow_executions",
                    {"workflow_execution_set": [workflow_execution]},
                )
            response = exc.value.response
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

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
        api_site_client_having_json_submit_allowance,
        seeded_db_having_workflow_execution_dependencies,
    ):
        """
        Submit a `WorkflowExecution` along with some `DataObject`s that it references,
        then confirm all of those documents now exist in the database.
        """

        # Get references to the database and relevant seeded data.
        db, seeded_data = seeded_db_having_workflow_execution_dependencies
        data_object_set = db.get_collection("data_object_set")
        workflow_execution_set = db.get_collection("workflow_execution_set")
        data_generation = seeded_data["data_generation"]
        workflow_execution_id = "nmdc:wfmgan-00-000003.1"

        # Generate a `WorkflowExecution` and its `DataObject`s and insert them into the database.
        faker = Faker()
        data_object_e, data_object_f = faker.generate_data_objects(quantity=2)
        data_object_e["id"] = "nmdc:dobj-00-000005"
        data_object_f["id"] = "nmdc:dobj-00-000006"
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
            response = api_site_client_having_json_submit_allowance.request(
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
