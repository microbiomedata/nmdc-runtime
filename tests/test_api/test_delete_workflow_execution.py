import pytest
import requests
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.db.mongo import get_mongo_db
from tests.lib.faker import Faker
from tests.test_api.test_endpoints import api_user_client


def test_delete_workflow_execution_cascade_deletion(api_user_client):
    """
    Test the DELETE /workflows/workflow_executions/{workflow_execution_id} endpoint.
    
    This test verifies that deleting a workflow execution properly cascades to:
    1. Delete the target workflow execution
    2. Delete all downstream workflow executions that depend on its outputs
    3. Delete all data objects that are outputs of deleted workflow executions
    4. Preserve `has_input` data objects that are not outputs of deleted workflow executions
    """
    faker = Faker()
    mdb: MongoDatabase = get_mongo_db()
    
    # Set up user permissions for delete operations
    #
    # TODO: Consider updating the `api_user_client` fixture to automatically grant
    #       this allowance to the test user. Tests can then revoke it if they happen
    #       to be testing unauthorized access (I think fewer tests need that
    #       allowance revoked than need it granted).
    #
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    allowances_collection.replace_one(allow_spec, allow_spec, upsert=True)
    
    # Collections we'll work with
    study_set = mdb.get_collection("study_set")
    biosample_set = mdb.get_collection("biosample_set")
    data_object_set = mdb.get_collection("data_object_set")
    data_generation_set = mdb.get_collection("data_generation_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    functional_annotation_agg = mdb.get_collection("functional_annotation_agg")
    
    # Generate foundational documents
    study = faker.generate_studies(quantity=1)[0]
    biosample = faker.generate_biosamples(quantity=1, associated_studies=[study["id"]])[0]
    data_generation = faker.generate_nucleotide_sequencings(
        quantity=1, associated_studies=[study["id"]], has_input=[biosample["id"]]
    )[0]
    
    # Generate data objects:
    # - input_data_objects: Will be inputs to primary workflow (should be preserved)
    # - primary_output_data_objects: Outputs of primary workflow (should be deleted)
    # - dependent1_output_data_objects: Outputs of dependent workflow 1 (should be deleted)
    # - dependent2_output_data_objects: Outputs of dependent workflow 2 (should be deleted)
    # - external_input_data_objects: External inputs to dependent workflows (should be preserved)
    input_data_objects = faker.generate_data_objects(quantity=2, name="input_data")
    primary_output_data_objects = faker.generate_data_objects(quantity=3, name="primary_output_data")
    dependent1_output_data_objects = faker.generate_data_objects(quantity=2, name="dependent1_output_data")
    dependent2_output_data_objects = faker.generate_data_objects(quantity=2, name="dependent2_output_data")
    external_input_data_objects = faker.generate_data_objects(quantity=1, name="external_input_data")
    
    # Generate workflow executions
    # Primary workflow execution (the one we'll delete) - MetagenomeAnnotation type triggers functional annotation deletion
    primary_workflow_execution = faker.generate_workflow_executions(
        quantity=1,
        workflow_type="nmdc:MetagenomeAnnotation",
        has_input=[obj["id"] for obj in input_data_objects],
        has_output=[obj["id"] for obj in primary_output_data_objects],
        was_informed_by=data_generation["id"]
    )[0]
    
    # Dependent workflow execution 1 (uses some outputs from primary as inputs)
    dependent_workflow_execution_1 = faker.generate_workflow_executions(
        quantity=1,
        workflow_type="nmdc:MetagenomeAssembly",
        has_input=[
            primary_output_data_objects[0]["id"],  # Uses primary output
            primary_output_data_objects[1]["id"],  # Uses primary output
            external_input_data_objects[0]["id"]   # Uses external input (should be preserved)
        ],
        has_output=[obj["id"] for obj in dependent1_output_data_objects],
        was_informed_by=data_generation["id"]
    )[0]
    
    # Dependent workflow execution 2 (uses some outputs from primary as inputs)
    dependent_workflow_execution_2 = faker.generate_workflow_executions(
        quantity=1,
        workflow_type="nmdc:ReadQcAnalysis",
        has_input=[
            primary_output_data_objects[2]["id"],  # Uses primary output
            external_input_data_objects[0]["id"]   # Uses external input (should be preserved)
        ],
        has_output=[obj["id"] for obj in dependent2_output_data_objects],
        was_informed_by=data_generation["id"]
    )[0]
    
    # Create some functional annotation records for the primary workflow (AnnotatingWorkflow)
    # TODO: Implement a `Faker` method that generates `FunctionalAnnotationAggMember` documents.
    functional_annotation_records = [
        {
            "gene_function_id": "PFAM:PF12705",
            "count": 4,
            "type": "nmdc:FunctionalAnnotationAggMember",
            "was_generated_by": primary_workflow_execution["id"]
        },
        {
            "gene_function_id": "PFAM:PF00001",
            "count": 2,
            "type": "nmdc:FunctionalAnnotationAggMember",
            "was_generated_by": primary_workflow_execution["id"]
        }
    ]
    
    # Insert all documents into database
    try:
        # Insert foundational documents
        study_set.insert_one(study)
        biosample_set.insert_one(biosample)
        data_generation_set.insert_one(data_generation)
        
        # Insert all data objects
        all_data_objects = (
            input_data_objects + 
            primary_output_data_objects + 
            dependent1_output_data_objects + 
            dependent2_output_data_objects + 
            external_input_data_objects
        )
        data_object_set.insert_many(all_data_objects)
        
        # Insert workflow executions
        workflow_execution_set.insert_many([
            primary_workflow_execution,
            dependent_workflow_execution_1,
            dependent_workflow_execution_2
        ])
        
        # Insert functional annotation records
        functional_annotation_agg.insert_many(functional_annotation_records)
        
        # Verify initial state
        assert workflow_execution_set.count_documents({}) >= 3
        assert data_object_set.count_documents({}) >= 8
        assert functional_annotation_agg.count_documents(
            {"was_generated_by": primary_workflow_execution["id"]}
        ) == 2
        
        # Execute the DELETE request
        response = api_user_client.request(
            "DELETE",
            f"/workflows/workflow_executions/{primary_workflow_execution['id']}"
        )
        
        # Verify successful response
        assert response.status_code == 200
        response_data = response.json()
        assert "message" in response_data
        assert "deleted_workflow_execution_ids" in response_data
        assert "deleted_data_object_ids" in response_data
        
        # Verify all 3 workflow executions were deleted
        deleted_wfe_ids = set(response_data["deleted_workflow_execution_ids"])
        expected_deleted_wfe_ids = {
            primary_workflow_execution["id"],
            dependent_workflow_execution_1["id"],
            dependent_workflow_execution_2["id"]
        }
        assert deleted_wfe_ids == expected_deleted_wfe_ids
        
        # Verify all output data objects were deleted
        deleted_data_object_ids = set(response_data["deleted_data_object_ids"])
        expected_deleted_data_object_ids = set(
            [obj["id"] for obj in primary_output_data_objects] +
            [obj["id"] for obj in dependent1_output_data_objects] +
            [obj["id"] for obj in dependent2_output_data_objects]
        )
        assert deleted_data_object_ids == expected_deleted_data_object_ids
        
        # Verify workflow executions are actually deleted from database
        for wfe_id in deleted_wfe_ids:
            assert workflow_execution_set.count_documents({"id": wfe_id}) == 0
        
        # Verify output data objects are actually deleted from database
        for data_obj_id in deleted_data_object_ids:
            assert data_object_set.count_documents({"id": data_obj_id}) == 0
        
        # Verify input data objects that are not outputs are preserved
        preserved_data_object_ids = (
            [obj["id"] for obj in input_data_objects] +
            [obj["id"] for obj in external_input_data_objects]
        )
        for data_obj_id in preserved_data_object_ids:
            assert data_object_set.count_documents({"id": data_obj_id}) == 1
        
        # Verify functional annotation records were deleted
        assert functional_annotation_agg.count_documents(
            {"was_generated_by": primary_workflow_execution["id"]}
        ) == 0
        
    finally:
        # Clean up any remaining test data
        study_set.delete_many({"id": study["id"]})
        biosample_set.delete_many({"id": biosample["id"]})
        data_generation_set.delete_many({"id": data_generation["id"]})
        
        # Clean up any remaining workflow executions
        all_wfe_ids = [
            primary_workflow_execution["id"],
            dependent_workflow_execution_1["id"],
            dependent_workflow_execution_2["id"]
        ]
        workflow_execution_set.delete_many({"id": {"$in": all_wfe_ids}})
        
        # Clean up any remaining data objects
        all_data_object_ids = [obj["id"] for obj in all_data_objects]
        data_object_set.delete_many({"id": {"$in": all_data_object_ids}})
        
        # Clean up functional annotation records
        functional_annotation_agg.delete_many({
            "gene_function_id": {"$in": [record["gene_function_id"] for record in functional_annotation_records]}
        })
        
        # Clean up permissions
        allowances_collection.delete_one(allow_spec)


def test_delete_workflow_execution_not_found(api_user_client):
    """
    Test that deleting a non-existent workflow execution returns 404.
    """
    mdb: MongoDatabase = get_mongo_db()
    
    # Set up user permissions for delete operations
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    allowances_collection.replace_one(allow_spec, allow_spec, upsert=True)
    
    non_existent_id = "nmdc:wfmgan-00-nonexistent.1"
    
    # Verify that the non-existent ID doesn't exist in the workflow_execution_set collection
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    assert workflow_execution_set.count_documents({"id": non_existent_id}) == 0, f"ID {non_existent_id} should not exist in database"
        
    try:
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            api_user_client.request(
                "DELETE",
                f"/workflows/workflow_executions/{non_existent_id}"
            )
        
        # Verify the exception details
        response = exc_info.value.response
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    finally:
        # Clean up permissions
        allowances_collection.delete_one(allow_spec)


def test_delete_workflow_execution_simple_case(api_user_client):
    """
    Test deleting a workflow execution with no dependencies.
    """
    faker = Faker()
    mdb: MongoDatabase = get_mongo_db()
    
    # Set up user permissions for delete operations
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    allowances_collection.replace_one(allow_spec, allow_spec, upsert=True)
    
    # Collections we'll work with
    study_set = mdb.get_collection("study_set")
    biosample_set = mdb.get_collection("biosample_set")
    data_object_set = mdb.get_collection("data_object_set")
    data_generation_set = mdb.get_collection("data_generation_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    
    # Generate simple test case: one workflow execution with input and output
    study = faker.generate_studies(quantity=1)[0]
    biosample = faker.generate_biosamples(quantity=1, associated_studies=[study["id"]])[0]
    data_generation = faker.generate_nucleotide_sequencings(
        quantity=1, associated_studies=[study["id"]], has_input=[biosample["id"]]
    )[0]
    
    input_data_object = faker.generate_data_objects(quantity=1, name="input_data")[0]
    output_data_object = faker.generate_data_objects(quantity=1, name="output_data")[0]
    
    workflow_execution = faker.generate_workflow_executions(
        quantity=1,
        workflow_type="nmdc:MetagenomeAssembly",
        has_input=[input_data_object["id"]],
        has_output=[output_data_object["id"]],
        was_informed_by=data_generation["id"]
    )[0]
    
    try:
        # Insert test data
        study_set.insert_one(study)
        biosample_set.insert_one(biosample)
        data_generation_set.insert_one(data_generation)
        data_object_set.insert_many([input_data_object, output_data_object])
        workflow_execution_set.insert_one(workflow_execution)
        
        # Verify initial state
        assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 1
        assert data_object_set.count_documents({"id": input_data_object["id"]}) == 1
        assert data_object_set.count_documents({"id": output_data_object["id"]}) == 1
        
        # Execute the DELETE request
        response = api_user_client.request(
            "DELETE",
            f"/workflows/workflow_executions/{workflow_execution['id']}"
        )
        
        # Verify successful response
        assert response.status_code == 200
        response_data = response.json()
        
        # Verify only the target workflow execution was deleted
        assert response_data["deleted_workflow_execution_ids"] == [workflow_execution["id"]]
        assert response_data["deleted_data_object_ids"] == [output_data_object["id"]]
        
        # Verify actual deletion from database
        assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 0
        assert data_object_set.count_documents({"id": output_data_object["id"]}) == 0
        
        # Verify input data object is preserved
        assert data_object_set.count_documents({"id": input_data_object["id"]}) == 1
        
    finally:
        # Clean up
        study_set.delete_many({"id": study["id"]})
        biosample_set.delete_many({"id": biosample["id"]})
        data_generation_set.delete_many({"id": data_generation["id"]})
        workflow_execution_set.delete_many({"id": workflow_execution["id"]})
        data_object_set.delete_many({"id": {"$in": [input_data_object["id"], output_data_object["id"]]}})
        
        # Clean up permissions
        allowances_collection.delete_one(allow_spec)


def test_delete_workflow_execution_unauthorized_user(api_user_client):
    """
    Test that deleting a workflow execution without proper permissions returns 403 Forbidden.
    """
    mdb: MongoDatabase = get_mongo_db()
    faker = Faker()
    
    # DON'T set up user permissions - this user should be unauthorized
    # This is the key difference from other tests
    
    # Verify that the allowances collection doesn't have delete permissions for this user
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    delete_permission = allowances_collection.find_one({
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)"
    })
    assert delete_permission is None, "User should not have delete permissions for this test"
    
    # Create a simple workflow execution to attempt to delete
    study = faker.generate_studies(quantity=1)[0]
    biosample = faker.generate_biosamples(quantity=1, associated_studies=[study["id"]])[0]
    data_generation = faker.generate_nucleotide_sequencings(
        quantity=1, associated_studies=[study["id"]], has_input=[biosample["id"]]
    )[0]
    
    input_data_object = faker.generate_data_objects(quantity=1, name="input_data")[0]
    output_data_object = faker.generate_data_objects(quantity=1, name="output_data")[0]
    
    workflow_execution = faker.generate_workflow_executions(
        quantity=1,
        workflow_type="nmdc:MetagenomeAssembly",
        has_input=[input_data_object["id"]],
        has_output=[output_data_object["id"]],
        was_informed_by=data_generation["id"]
    )[0]
    
    # Collections for setup and cleanup
    study_set = mdb.get_collection("study_set")
    biosample_set = mdb.get_collection("biosample_set")
    data_object_set = mdb.get_collection("data_object_set")
    data_generation_set = mdb.get_collection("data_generation_set")
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    
    try:
        # Insert test data
        study_set.insert_one(study)
        biosample_set.insert_one(biosample)
        data_generation_set.insert_one(data_generation)
        data_object_set.insert_many([input_data_object, output_data_object])
        workflow_execution_set.insert_one(workflow_execution)
                
        # Attempt to delete workflow execution without permissions
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            api_user_client.request(
                "DELETE",
                f"/workflows/workflow_executions/{workflow_execution['id']}"
            )
        
        # Verify the exception details
        response = exc_info.value.response
        assert response.status_code == 403
        
        # Verify that nothing was actually deleted (workflow execution should still exist)
        assert workflow_execution_set.count_documents({"id": workflow_execution["id"]}) == 1
        assert data_object_set.count_documents({"id": input_data_object["id"]}) == 1
        assert data_object_set.count_documents({"id": output_data_object["id"]}) == 1
        
    finally:
        # Clean up test data
        study_set.delete_many({"id": study["id"]})
        biosample_set.delete_many({"id": biosample["id"]})
        data_generation_set.delete_many({"id": data_generation["id"]})
        workflow_execution_set.delete_many({"id": workflow_execution["id"]})
        data_object_set.delete_many({"id": {"$in": [input_data_object["id"], output_data_object["id"]]}})
