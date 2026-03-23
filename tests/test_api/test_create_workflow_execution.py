import re
from typing import List, Optional

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
    ineligible_ids: Optional[List[str]] = None,
) -> str:
    """
    Generates an ID beginning with the specified prefix (and optional suffix), that is not already
    in use by any documents in the specified Mongo collection and is not among the ineligible IDs.
    
    The `suffix` parameter can be used to append a ".1" (for example) to the ID of a
    `WorkflowExecution`, given that NMDC workflow automation team members normally
    append ".{integer}" to minted `WorkflowExecution` IDs.
    
    The `ineligible_ids` parameter can be used to specify IDs that you do not want this invocation
    to generate (maybe because it already generated those IDs).
    """
    if ineligible_ids is None:
        ineligible_ids = []

    n = 1
    available_id = f"{prefix}-00-{n:06d}{suffix}"  # e.g. "nmdc:wfmgan-00-000001"
    while available_id in ineligible_ids or collection.count_documents({"id": available_id}, limit=1) != 0:
        n += 1
        available_id = f"{prefix}-00-{n:06d}{suffix}"  # e.g. "nmdc:wfmgan-00-000002"
    return available_id


class TestPostWorkflowWorkflowExecutions:
    """Tests targeting the `POST /workflows/workflow_executions` API endpoint."""

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
        data_generation = faker.generate_nucleotide_sequencings(
            quantity=1, associated_studies=[study["id"]], has_input=[biosample["id"]]
        )[0]

        # Get references to relevant Mongo collections.
        db = get_mongo_db()
        study_set = db.get_collection("study_set")
        biosample_set = db.get_collection("biosample_set")
        data_object_set = db.get_collection("data_object_set")
        data_generation_set = db.get_collection("data_generation_set")

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

        # Yield the database and seeded documents.
        yield (
            db,
            {
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
        data_object_set.delete_many(
            {"id": {"$in": [data_object_a["id"], data_object_b["id"], data_object_c["id"], data_object_d["id"]]}}
        )
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
        workflow_execution_id = generate_available_id_for_collection(
            workflow_execution_set, "nmdc:wfmgan", ".1"
        )

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

    def test_it_inserts_workflow_execution(
        self,
        api_site_client,
        seeded_db_having_workflow_execution_dependencies,
    ):
        """Submit a valid WFE to the API endpoint, then confirm it exists in the database."""

        # Get references to the database and relevant seeded data.
        db, seeded_data = seeded_db_having_workflow_execution_dependencies
        data_object_a = seeded_data["data_object_a"]
        data_object_b = seeded_data["data_object_b"]
        data_generation = seeded_data["data_generation"]
        workflow_execution_set = db.get_collection("workflow_execution_set")
        workflow_execution_id = generate_available_id_for_collection(
            workflow_execution_set, "nmdc:wfmgan", ".1"
        )

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
        then confirm the endpoint responds with an HTTP 422 status due to the broken reference.
        """

        # Get references to the database and relevant seeded data.
        db, seeded_data = seeded_db_having_workflow_execution_dependencies
        data_object_a = seeded_data["data_object_a"]
        data_object_b = seeded_data["data_object_b"]
        data_generation_set = db.get_collection("data_generation_set")
        workflow_execution_set = db.get_collection("workflow_execution_set")
        workflow_execution_id = generate_available_id_for_collection(
            workflow_execution_set, "nmdc:wfmgan", ".1"
        )
        data_generation_id = generate_available_id_for_collection(
            data_generation_set, "nmdc:dgns"
        )

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
        Submit a `WorkflowExecution` along with some `DataObject`s that it references,
        then confirm all of those documents now exist in the database.
        """

        # Get references to the database and relevant seeded data.
        db, seeded_data = seeded_db_having_workflow_execution_dependencies
        data_object_set = db.get_collection("data_object_set")
        workflow_execution_set = db.get_collection("workflow_execution_set")
        data_generation = seeded_data["data_generation"]
        workflow_execution_id = generate_available_id_for_collection(
            workflow_execution_set, "nmdc:wfmgan", ".1"
        )

        # Generate a `WorkflowExecution` and its `DataObject`s and insert them into the database.
        faker = Faker()
        data_object_e, data_object_f = faker.generate_data_objects(quantity=2)
        data_object_e["id"] = generate_available_id_for_collection(data_object_set, "nmdc:dobj")
        data_object_f["id"] = generate_available_id_for_collection(data_object_set, "nmdc:dobj", ineligible_ids=[data_object_e["id"]])
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
        Given a database containing a WFE and its output DOBJs, submit a WFE whose `id` value
        indicates that it supersedes that existing WFE. Confirm the `superseded_by` fields of
        the "existing" WFE and its output DOBJs reference the new one.
        """
        db, seeded_data = seeded_db_having_workflow_execution
        workflow_execution_set = db.get_collection("workflow_execution_set")
        data_object_set = db.get_collection("data_object_set")

        faker = Faker()
        later_wfe_id = seeded_data["workflow_execution_id"].replace(".1", ".2")
        assert workflow_execution_set.count_documents({"id": later_wfe_id}) == 0
        later_wfe = faker.generate_metagenome_annotations(
            quantity=1,
            id=later_wfe_id,
            has_input=[seeded_data["data_object_a"]["id"]],
            has_output=[seeded_data["data_object_b"]["id"]],  # <-- this is the output DOBJ
            was_informed_by=[seeded_data["data_generation"]["id"]],
        )[0]

        try:
            # Confirm the existing WFE and its output DOBJs do not have `superseded_by` values yet.
            assert "superseded_by" not in seeded_data["workflow_execution"]
            assert "superseded_by" not in seeded_data["data_object_b"]

            # Submit an API request whose payload contains the superseding WFE.
            response = api_site_client.request(
                "POST",
                "/workflows/workflow_executions",
                {
                    "workflow_execution_set": [later_wfe],
                },
            )
            assert response.status_code == status.HTTP_200_OK
            response_message = response.json()["message"]
            assert re.search(r"^Inserted 1 documents$", response_message) is not None

            # Confirm that the pre-existing WFE and its output DOBJ each have a `superseded_by`
            # field that references the newly-inserted superseding WFE.
            earlier_wfe = workflow_execution_set.find_one({"id": seeded_data["workflow_execution"]["id"]})
            assert earlier_wfe["superseded_by"] == later_wfe["id"]
            data_object_b = data_object_set.find_one({"id": seeded_data["data_object_b"]["id"]})
            assert data_object_b["superseded_by"] == later_wfe["id"]

            # Assert that the other seeded DOBJs still have no `superseded_by` fields.
            data_object_a = data_object_set.find_one({"id": seeded_data["data_object_a"]["id"]})
            assert "superseded_by" not in data_object_a
            data_object_c = data_object_set.find_one({"id": seeded_data["data_object_c"]["id"]})
            assert "superseded_by" not in data_object_c
            data_object_d = data_object_set.find_one({"id": seeded_data["data_object_d"]["id"]})
            assert "superseded_by" not in data_object_d
        finally:
            # Delete the superseding WFE that we created.
            workflow_execution_set.delete_many({"id": later_wfe["id"]})

    def test_it_updates_superseded_by_fields_of_co_submitted_workflow_executions_and_data_objects(
        self,
        api_site_client,
        seeded_db_having_workflow_execution_dependencies,
    ):
        """
        Submit a pair of WFEs—one whose ID indicates that it supersedes the other—and confirm that
        the superseded WFE has its `superseded_by` field set to point to the superseding WFE. Also
        confirm that the submitted DOBJs that are outputs of the superseded WFE have their
        `superseded_by` fields updated to point to the superseding WFE.
        """
        db, seeded_data = seeded_db_having_workflow_execution_dependencies
        workflow_execution_set = db.get_collection("workflow_execution_set")
        data_object_set = db.get_collection("data_object_set")
        data_generation_id = seeded_data["data_generation"]["id"]

        # Generate the WFEs and DOBJs.
        faker = Faker()
        wfe_id_1 = "nmdc:wfmgan-00-000001.1"
        wfe_id_2 = "nmdc:wfmgan-00-000001.2"
        dobj_id_1 = "nmdc:dobj-99-000001"
        dobj_id_2 = "nmdc:dobj-99-000002"
        dobj_id_3 = "nmdc:dobj-99-000003"
        dobj_id_4 = "nmdc:dobj-99-000004"
        wfe_1, wfe_2 = faker.generate_metagenome_annotations(
            quantity=2,
            id=wfe_id_1,
            has_input=[dobj_id_1],
            has_output=[dobj_id_2],
            was_informed_by=[data_generation_id],
        )
        wfe_2["id"] = wfe_id_2
        wfe_2["has_input"] = [dobj_id_3]
        wfe_2["has_output"] = [dobj_id_4]
        dobj_1, dobj_2, dobj_3, dobj_4 = faker.generate_data_objects(quantity=4)
        dobj_1["id"] = dobj_id_1
        dobj_2["id"] = dobj_id_2
        dobj_3["id"] = dobj_id_3
        dobj_4["id"] = dobj_id_4

        try:
            assert workflow_execution_set.count_documents({"id": {"$in": [wfe_id_1, wfe_id_2]}}) == 0
            assert data_object_set.count_documents({"id": {"$in": [dobj_id_1, dobj_id_2, dobj_id_3, dobj_id_4]}}) == 0

            # Submit an API request whose payload contains both the superseding and superseded WFEs.
            response = api_site_client.request(
                "POST",
                "/workflows/workflow_executions",
                {
                    "workflow_execution_set": [wfe_1, wfe_2],
                    "data_object_set": [dobj_1, dobj_2, dobj_3, dobj_4],
                },
            )
            assert response.status_code == status.HTTP_200_OK
            response_message = response.json()["message"]
            assert re.search(r"^Inserted 6 documents$", response_message) is not None

            # Confirm the newly-inserted, superseded WFE and its output DOBJ have their `superseded_by`
            # field set to point to the superseding WFE.
            wfe_1_from_db = workflow_execution_set.find_one({"id": wfe_id_1})
            assert wfe_1_from_db["superseded_by"] == wfe_id_2
            dobj_2_from_db = data_object_set.find_one({"id": dobj_id_2})
            assert dobj_2_from_db["superseded_by"] == wfe_id_2

            # Confirm the co-submitted superseding WFE and its output DOBJ do not have any `superseded_by` field.
            wfe_2_from_db = workflow_execution_set.find_one({"id": wfe_id_2})
            assert "superseded_by" not in wfe_2_from_db
            dobj_4_from_db = data_object_set.find_one({"id": dobj_id_4})
            assert "superseded_by" not in dobj_4_from_db
        finally:
            # Delete the WFEs and DOBJs that the API created.
            workflow_execution_set.delete_many({"id": {"$in": [wfe_id_1, wfe_id_2]}})
            data_object_set.delete_many({"id": {"$in": [dobj_id_1, dobj_id_2, dobj_id_3, dobj_id_4]}})
