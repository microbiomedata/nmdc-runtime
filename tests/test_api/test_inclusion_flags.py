from json import dumps

import pytest
import requests
from starlette import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.config import IS_LINKED_INSTANCES_ENDPOINT_ENABLED
from nmdc_runtime.site.ops.alldocs import materialize_alldocs
from tests.lib.faker import Faker


class TestInclusionFlags:
    """
    An assortment of tests focused on the `include_superseded` and `include_failed` flags that are
    supported by some API endpoints.
    """

    study_ids = [
        "nmdc:sty-00-001001",
    ]
    biosample_ids = [
        "nmdc:bsm-00-001001",
    ]
    data_generation_ids = [
        "nmdc:dgns-00-001001",
    ]
    data_object_ids = [
        "nmdc:dobj-00-001001",
        "nmdc:dobj-00-001002",
        "nmdc:dobj-00-002001",
        "nmdc:dobj-00-003001",
    ]
    workflow_execution_ids = [
        "nmdc:wfmga-00-001001.1",
        "nmdc:wfmga-00-001001.2",
        "nmdc:wfmga-00-001002.1",
    ]

    @pytest.fixture
    def db_seeded_for_inclusion_flag_testing(self, op_context):
        """
        Seeds the database, refreshes the `alldocs` cache collection, yields (for the dependent test
        to run), then deletes those documents from the database and refreshes `alldocs` again.
        Also deletes any newly-created pagination tokens from the database.

        Seeded data:
        - Study             "nmdc:sty-00-001001"
        - Biosample         "nmdc:bsm-00-001001"     is associated with Study
        - DataGeneration    "nmdc:dgns-00-001001"    is associated with Study, has Biosample as input, has DataObject "002001" as output
        - DataObject        "nmdc:dobj-00-002001"    (output of DataGeneration)
        - WorkflowExecution "nmdc:wfmga-00-001001.1" is superseded, has DataObject "001002" as output
        - WorkflowExecution "nmdc:wfmga-00-001001.2" has DataObject "001001" as output
        - WorkflowExecution "nmdc:wfmga-00-001002.1" is failed, has DataObject "003001" as output
        - DataObject        "nmdc:dobj-00-001001"    (output of non-superseded, non-failed WorkflowExecution)
        - DataObject        "nmdc:dobj-00-001002"    is superseded (and output of superseded WorkflowExecution)
        - DataObject        "nmdc:dobj-00-003001"    (output of failed WorkflowExecution)
        
        Note: `op_context` is a pytest fixture (it's currently defined in the `conftest.py` file).
        """

        faker = Faker()

        # Get references to relevant MongoDB collections.
        db = get_mongo_db()
        study_set = db.get_collection("study_set")
        biosample_set = db.get_collection("biosample_set")
        data_generation_set = db.get_collection("data_generation_set")
        workflow_execution_set = db.get_collection("workflow_execution_set")
        data_object_set = db.get_collection("data_object_set")
        page_tokens = db.get_collection("page_tokens")
        query_continuations = db.get_collection("_runtime.query_continuations")

        # Generate Study.
        study = faker.generate_studies(1, id=self.study_ids[0])[0]

        # Generate Biosample.
        biosample = faker.generate_biosamples(
            1,
            id=self.biosample_ids[0],
            associated_studies=[study["id"]],
        )[0]

        # Generate DataGeneration.
        data_generation = faker.generate_nucleotide_sequencings(
            1,
            id=self.data_generation_ids[0],
            associated_studies=[study["id"]],
            has_input=[biosample["id"]],
            has_output=[self.data_object_ids[2]]
        )[0]

        # Generate DataObjects.
        data_objects = [{}, {}, {}, {}]
        data_objects[0] = faker.generate_data_objects(
            1,
            id=self.data_object_ids[0],
        )[0]
        data_objects[1] = faker.generate_data_objects(
            1,
            id=self.data_object_ids[1],
            superseded_by=data_objects[0]["id"],  # 👋
        )[0]
        data_objects[2] = faker.generate_data_objects(
            1,
            id=self.data_object_ids[2],
        )[0]
        data_objects[3] = faker.generate_data_objects(
            1,
            id=self.data_object_ids[3],
        )[0]

        # Generate WorkflowExecutions.
        workflow_executions = [{}, {}, {}]
        workflow_executions[0] = faker.generate_workflow_executions(
            1,
            id=self.workflow_execution_ids[0],
            workflow_type="nmdc:MetagenomeAnnotation",
            was_informed_by=[data_generation["id"]],
            has_input=[data_objects[2]["id"]],
            has_output=[data_objects[1]["id"]],
            superseded_by=self.workflow_execution_ids[1],  # 👋
        )[0]
        workflow_executions[1] = faker.generate_workflow_executions(
            1,
            id=self.workflow_execution_ids[1],
            workflow_type="nmdc:MetagenomeAnnotation",
            was_informed_by=[data_generation["id"]],
            has_input=[data_objects[2]["id"]],
            has_output=[data_objects[0]["id"]],
        )[0]
        workflow_executions[2] = faker.generate_workflow_executions(
            1,
            id=self.workflow_execution_ids[2],
            workflow_type="nmdc:MetagenomeAnnotation",
            was_informed_by=[data_generation["id"]],
            has_input=[data_objects[2]["id"]],
            has_output=[data_objects[3]["id"]],
            qc_status="fail",  # 👋
        )[0]

        # Preserve the `_id` values of all existing pagination tokens.
        initial_page_token_oids = [
            doc["_id"] for doc in page_tokens.find({}, {"_id": 1})
        ]
        initial_query_continuation_oids = [
            doc["_id"] for doc in query_continuations.find({}, {"_id": 1})
        ]
        try:
            # Seed the database.
            study_set.insert_many([study])
            biosample_set.insert_many([biosample])
            data_generation_set.insert_many([data_generation])
            workflow_execution_set.insert_many(workflow_executions)
            data_object_set.insert_many(data_objects)
            materialize_alldocs(op_context)
            yield
        finally:
            # Delete the seeded data and any new pagination tokens that were created.
            study_set.delete_many({"id": {"$in": self.study_ids}})
            biosample_set.delete_many({"id": {"$in": self.biosample_ids}})
            data_generation_set.delete_many({"id": {"$in": self.data_generation_ids}})
            workflow_execution_set.delete_many(
                {"id": {"$in": self.workflow_execution_ids}}
            )
            data_object_set.delete_many({"id": {"$in": self.data_object_ids}})
            page_tokens.delete_many({"_id": {"$nin": initial_page_token_oids}})
            query_continuations.delete_many(
                {"_id": {"$nin": initial_query_continuation_oids}}
            )
            materialize_alldocs(op_context)

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
        assert {d["id"] for d in documents} == {
            "nmdc:dobj-00-001001",
            "nmdc:dobj-00-002001",
            "nmdc:dobj-00-003001",
        }

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
            "nmdc:dobj-00-002001",
            "nmdc:dobj-00-003001",
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

    def test_get_data_objects_for_study_endpoint(
        self, api_user_client, db_seeded_for_inclusion_flag_testing
    ):
        def fetch_data_object_ids(params: dict | None = None) -> set[str]:
            """
            Helper function that submits an API request and returns the DataObject IDs
            under the (only) Biosample represented in the API response body.
            """
            body = api_user_client.request(
                "GET",
                f"/data_objects/study/{self.study_ids[0]}",
                params,
            ).json()
            assert len(body) == 1
            assert body[0]["biosample_id"] == self.biosample_ids[0]
            return {d["id"] for d in body[0]["data_objects"]}

        # Test 1: By default, DataObjects only related to the Study via superseded or failed things
        #         are not included in the response.
        assert fetch_data_object_ids() == {
            "nmdc:dobj-00-001001",
            "nmdc:dobj-00-002001",
        }

        # Test 2: When `include_superseded` is true, DataObjects related to the Study via superseded
        #         things are included in the response, but those related via failed things aren't.
        assert fetch_data_object_ids({"include_superseded": True}) == {
            "nmdc:dobj-00-001001",
            "nmdc:dobj-00-001002",
            "nmdc:dobj-00-002001",
        }

        # Test 3: When `include_failed` is true, DataObjects related to the Study via failed
        #         things are included in the response, but those related via superseded things aren't.
        assert fetch_data_object_ids({"include_failed": True}) == {
            "nmdc:dobj-00-001001",
            "nmdc:dobj-00-002001",
            "nmdc:dobj-00-003001",
        }

        # Test 4: Confirm that, when both `include_superseded` and `include_failed` are set, all
        #         related DataObjects are included in the response.
        assert fetch_data_object_ids(
            {"include_superseded": True, "include_failed": True}
        ) == {
            "nmdc:dobj-00-001001",
            "nmdc:dobj-00-001002",
            "nmdc:dobj-00-002001",
            "nmdc:dobj-00-003001",
        }

    @pytest.mark.skipif(
        not IS_LINKED_INSTANCES_ENDPOINT_ENABLED,
        reason="Target endpoint is disabled",
    )
    def test_get_linked_instances_endpoint(
        self, api_user_client, db_seeded_for_inclusion_flag_testing
    ):
        def fetch_linked_instance_ids(
            starting_id: str, params: dict | None = None
        ) -> set[str]:
            """
            Helper function that submits an API request and returns the IDs of all
            "instances" (i.e. documents) represented in the API response body.
            """
            query_params = {
                "ids": [starting_id],
                "max_page_size": 200,
                **(params or {}),
            }
            body = api_user_client.request(
                "GET",
                "/nmdcschema/linked_instances",
                query_params,
            ).json()
            assert body.get("next_page_token") is None
            return {r["id"] for r in body["resources"]}

        study_id = self.study_ids[0]

        # The `id` values of instances that are neither superseded nor failed, and are reachable
        # from the seeded study via instances that are neither superseded nor failed.
        ids_linked_via_normal_instances = {
            self.biosample_ids[0],
            self.data_generation_ids[0],
            "nmdc:dobj-00-002001",
            "nmdc:wfmga-00-001001.2",
            "nmdc:dobj-00-001001",
        }

        # The `id` values of instances that are superseded and/or are only reachable
        # from the seeded study via instances that are superseded.
        ids_linked_via_superseded_instances = {
            "nmdc:wfmga-00-001001.1",
            "nmdc:dobj-00-001002",
        }

        # The `id` values of instances that are failed and/or are only reachable
        # from the seeded study via instances that are failed.
        ids_linked_via_failed_instances = {
            "nmdc:wfmga-00-001002.1",
            "nmdc:dobj-00-003001",
        }

        # Test 1: By default, the response does not include superseded and failed instances
        #         (and instances only reachable via them).
        assert fetch_linked_instance_ids(study_id) == ids_linked_via_normal_instances

        # Test 2: When `include_superseded` is true, the response includes superseded instances
        #         (and instances only reachable via them) in addition to the baseline result.
        assert (
            fetch_linked_instance_ids(study_id, params={"include_superseded": True})
            == ids_linked_via_normal_instances | ids_linked_via_superseded_instances
        )

        # Test 3: When `include_failed` is true, the response includes failed instances
        #         (and instances only reachable via them) in addition to the baseline result.
        assert (
            fetch_linked_instance_ids(study_id, params={"include_failed": True})
            == ids_linked_via_normal_instances | ids_linked_via_failed_instances
        )

        # Test 4: When both `include_superseded` and `include_failed` are true, all
        #         linked instances are included in the response.
        assert (
            fetch_linked_instance_ids(
                starting_id=study_id,
                params={"include_superseded": True, "include_failed": True}
            )
            == ids_linked_via_normal_instances | ids_linked_via_superseded_instances | ids_linked_via_failed_instances
        )

        # Test 5: By default, when trying to start from a superseded or failed instance, the
        #         endpoint returns an HTTP 404 response. The endpoint will only start from such
        #         an instance when the request includes the relevant inclusion flag.
        with pytest.raises(requests.HTTPError) as exc_info:
            fetch_linked_instance_ids("nmdc:wfmga-00-001001.1")
        assert exc_info.value.response.status_code == status.HTTP_404_NOT_FOUND
        with pytest.raises(requests.HTTPError) as exc_info:
            fetch_linked_instance_ids(starting_id="nmdc:wfmga-00-001002.1")
        assert exc_info.value.response.status_code == status.HTTP_404_NOT_FOUND
        assert (
            fetch_linked_instance_ids(
                starting_id="nmdc:wfmga-00-001001.1",
                params={"include_superseded": True},
            )
            == (ids_linked_via_normal_instances | {study_id} | ids_linked_via_superseded_instances)
            - {"nmdc:wfmga-00-001001.1"}
        )
        assert fetch_linked_instance_ids(
            starting_id="nmdc:wfmga-00-001002.1",
            params={"include_failed": True},
        ) == {
            study_id,
            self.biosample_ids[0],
            self.data_generation_ids[0],
            "nmdc:dobj-00-002001",
            "nmdc:dobj-00-003001",
        }
