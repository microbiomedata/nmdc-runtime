from json import dumps

import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from tests.lib.faker import Faker


class TestInclusionFlags:
    """
    An assortment of tests focused on the `include_superseded` and `include_failed` flags that are
    supported by some API endpoints.
    """

    data_object_ids = ["nmdc:dobj-00-001001", "nmdc:dobj-00-001002"]
    workflow_execution_ids = [
        "nmdc:wfmga-00-001001.1",
        "nmdc:wfmga-00-001001.2",
        "nmdc:wfmga-00-001002.1",
    ]

    @pytest.fixture
    def db_seeded_for_inclusion_flag_testing(self):
        """
        Generates workflow execution and data object documents, stores them in the database,
        yields (for the dependent test to run), then deletes those documents from the database.
        Also deletes any newly-created pagination tokens from the database.

        Summary of seeded data:
        - WFE "nmdc:wfmga-00-001001.1" is superseded
        - WFE "nmdc:wfmga-00-001001.2" is not superseded
        - WFE "nmdc:wfmga-00-001002.1" is not superseded, but is failed
        - DOBJ "nmdc:dobj-00-001001" is not superseded
        - DOBJ "nmdc:dobj-00-001002" is superseded
        """

        faker = Faker()

        # Get references to relevant MongoDB collections.
        db = get_mongo_db()
        workflow_execution_set = db.get_collection("workflow_execution_set")
        data_object_set = db.get_collection("data_object_set")
        page_tokens = db.get_collection("page_tokens")
        query_continuations = db.get_collection("_runtime.query_continuations")

        # Generate data objects.
        data_objects = faker.generate_data_objects(2)
        data_objects[0]["id"] = self.data_object_ids[0]
        data_objects[1]["id"] = self.data_object_ids[1]
        data_objects[1]["superseded_by"] = data_objects[0]["id"]

        # Generate workflow executions.
        workflow_executions = faker.generate_workflow_executions(
            3,
            workflow_type="nmdc:MetagenomeAnnotation",
            was_informed_by=["nmdc:dgns-00-000001"],
            has_input=[data_objects[0]["id"]],
        )
        workflow_executions[0]["id"] = self.workflow_execution_ids[0]
        workflow_executions[1]["id"] = self.workflow_execution_ids[1]
        workflow_executions[2]["id"] = self.workflow_execution_ids[2]
        workflow_executions[0]["superseded_by"] = workflow_executions[1]["id"]
        workflow_executions[2]["qc_status"] = "fail"

        # Preserve the `_id` values of all existing pagination tokens.
        initial_page_token_oids = [
            doc["_id"] for doc in page_tokens.find({}, {"_id": 1})
        ]
        initial_query_continuation_oids = [
            doc["_id"] for doc in query_continuations.find({}, {"_id": 1})
        ]
        try:
            # Seed the database with the workflow executions and data objects.
            workflow_execution_set.insert_many(workflow_executions)
            data_object_set.insert_many(data_objects)
            yield
        finally:
            # Delete the seeded data and any new pagination tokens that were created.
            workflow_execution_set.delete_many(
                {"id": {"$in": self.workflow_execution_ids}}
            )
            data_object_set.delete_many({"id": {"$in": self.data_object_ids}})
            page_tokens.delete_many({"_id": {"$nin": initial_page_token_oids}})
            query_continuations.delete_many(
                {"_id": {"$nin": initial_query_continuation_oids}}
            )

    def test_queries_run_endpoint_with_find_command(
        self, api_user_client, db_seeded_for_inclusion_flag_testing
    ):
        filter_ = {"id": {"$in": self.workflow_execution_ids}}

        # Confirm superseded WFEs and failed WFEs are excluded by default.
        response = api_user_client.request(
            "POST",
            "/queries:run",
            {"find": "workflow_execution_set", "filter": filter_},
        ).json()
        documents = response["cursor"]["batch"]
        assert {d["id"] for d in documents} == {"nmdc:wfmga-00-001001.2"}

        # Confirm superseded WFEs are included when the relevant inclusion flag is set.
        response = api_user_client.request(
            "POST",
            "/queries:run?include_superseded=true",
            {"find": "workflow_execution_set", "filter": filter_},
        ).json()
        documents = response["cursor"]["batch"]
        assert {d["id"] for d in documents} == {
            "nmdc:wfmga-00-001001.2",
            "nmdc:wfmga-00-001001.1",
        }

        # Confirm failed WFEs are included when the relevant inclusion flag is set.
        response = api_user_client.request(
            "POST",
            "/queries:run?include_failed=true",
            {"find": "workflow_execution_set", "filter": filter_},
        ).json()
        documents = response["cursor"]["batch"]
        assert {d["id"] for d in documents} == {
            "nmdc:wfmga-00-001001.2",
            "nmdc:wfmga-00-001002.1",
        }

        # Confirm that, when using `getMore`, the inclusion flags from the initial `find` are used.
        # Note: We set `batchSize` to a number smaller than the total being found so they span pages.
        response = api_user_client.request(
            "POST",
            "/queries:run?include_superseded=true&include_failed=true",
            {"find": "workflow_execution_set", "filter": filter_, "batchSize": 2},
        ).json()
        documents_a = response["cursor"]["batch"]
        token_a = response["cursor"]["id"]
        response = api_user_client.request(
            "POST",
            "/queries:run",
            {"getMore": token_a},
        ).json()
        documents_b = response["cursor"]["batch"]
        token_b = response["cursor"]["id"]
        documents = documents_a + documents_b
        assert token_b is None
        assert {document["id"] for document in documents} == {
            "nmdc:wfmga-00-001001.1",
            "nmdc:wfmga-00-001001.2",
            "nmdc:wfmga-00-001002.1",
        }

    def test_get_data_objects_endpoint(
        self, api_user_client, db_seeded_for_inclusion_flag_testing
    ):
        # Make a filter that matches a data object that happens to be superseded.
        # Note: This syntax really is what the endpoint was designed to deal with.
        filter_ = r"id:nmdc:dobj-00-001002"

        # Confirm that, by default, superseded data objects are omitted.
        response = api_user_client.request(
            "GET", "/data_objects", {"filter": filter_}
        ).json()
        documents = response["results"]
        assert {d["id"] for d in documents} == set()

        # Confirm that, when `include_superseded` is set, superseded data objects
        # are not omitted.
        response = api_user_client.request(
            "GET", "/data_objects", {"filter": filter_, "include_superseded": True}
        ).json()
        documents = response["results"]
        assert {d["id"] for d in documents} == {"nmdc:dobj-00-001002"}

    def test_get_planned_processes_endpoint(
        self, api_user_client, db_seeded_for_inclusion_flag_testing
    ):
        # Make a filter that matches a workflow execution that happens to be superseded.
        # Note: This syntax really is what the endpoint was designed to deal with.
        filter_ = r"id:nmdc:wfmga-00-001001.1"

        # Confirm that, by default, superseded workflow executions are omitted.
        response = api_user_client.request(
            "GET", "/planned_processes", {"filter": filter_}
        ).json()
        documents = response["results"]
        assert {d["id"] for d in documents} == set()

        # Confirm that, when `include_superseded` is set, superseded workflow executions
        # are not omitted.
        response = api_user_client.request(
            "GET", "/planned_processes", {"filter": filter_, "include_superseded": True}
        ).json()
        documents = response["results"]
        assert {d["id"] for d in documents} == {"nmdc:wfmga-00-001001.1"}

        # Confirm that, by default, failed workflow executions are omitted.
        filter_ = r"id:nmdc:wfmga-00-001002.1"
        response = api_user_client.request(
            "GET", "/planned_processes", {"filter": filter_}
        ).json()
        documents = response["results"]
        assert {d["id"] for d in documents} == set()

        # Confirm that, when `include_failed` is set, failed workflow executions
        # are not omitted.
        response = api_user_client.request(
            "GET", "/planned_processes", {"filter": filter_, "include_failed": True}
        ).json()
        documents = response["results"]
        assert {d["id"] for d in documents} == {"nmdc:wfmga-00-001002.1"}

    def test_get_nmdcschema_collection_endpoint(
        self, api_user_client, db_seeded_for_inclusion_flag_testing
    ):
        # Confirm that, by default, superseded data objects are omitted.
        filter_ = dumps({"id": {"$in": self.data_object_ids}})
        response = api_user_client.request(
            "GET", "/nmdcschema/data_object_set", {"filter": filter_}
        ).json()
        documents = response["resources"]
        assert {d["id"] for d in documents} == {"nmdc:dobj-00-001001"}

        # Confirm that, when `include_superseded` is set, superseded data objects
        # are not omitted.
        response = api_user_client.request(
            "GET",
            "/nmdcschema/data_object_set",
            {"filter": filter_, "include_superseded": True},
        ).json()
        documents = response["resources"]
        assert {d["id"] for d in documents} == {
            "nmdc:dobj-00-001001",
            "nmdc:dobj-00-001002",
        }

        # Confirm that, by default, superseded and failed workflow executions are omitted.
        filter_ = dumps({"id": {"$in": self.workflow_execution_ids}})
        response = api_user_client.request(
            "GET", "/nmdcschema/workflow_execution_set", {"filter": filter_}
        ).json()
        documents = response["resources"]
        assert {d["id"] for d in documents} == {"nmdc:wfmga-00-001001.2"}

        # Confirm that, when `include_superseded` is set, superseded workflow executions
        # are not omitted.
        response = api_user_client.request(
            "GET",
            "/nmdcschema/workflow_execution_set",
            {"filter": filter_, "include_superseded": True},
        ).json()
        documents = response["resources"]
        assert {d["id"] for d in documents} == {
            "nmdc:wfmga-00-001001.2",
            "nmdc:wfmga-00-001001.1",
        }

        # Confirm that, when `include_failed` is set, failed workflow executions
        # are not omitted.
        response = api_user_client.request(
            "GET",
            "/nmdcschema/workflow_execution_set",
            {"filter": filter_, "include_failed": True},
        ).json()
        documents = response["resources"]
        assert {d["id"] for d in documents} == {
            "nmdc:wfmga-00-001001.2",
            "nmdc:wfmga-00-001002.1",
        }
