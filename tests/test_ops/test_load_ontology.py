import os
import pytest
import tempfile
import shutil
from unittest.mock import patch, MagicMock, ANY
from dagster import build_op_context
from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.site.ops import load_ontology


@pytest.fixture
def client_config():
    return {
        "dbname": os.getenv("MONGO_DBNAME"),
        "host": os.getenv("MONGO_HOST"),
        "password": os.getenv("MONGO_PASSWORD"),
        "username": os.getenv("MONGO_USERNAME"),
    }


@pytest.fixture
def temp_directory():
    # Create a temporary directory for test output
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up after test
    shutil.rmtree(temp_dir)


@pytest.fixture
def op_context(client_config, temp_directory):
    return build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)},
        op_config={
            "source_ontology": "envo",
            "output_directory": temp_directory,
            "generate_reports": False
        }
    )


# Method 1: Using mocks for unit testing (faster, doesn't require external dependencies)
@patch('nmdc_runtime.site.ops.OntologyLoaderController')
def test_load_ontology_mocked(mock_ontology_loader, op_context):
    """Tests the load_ontology op using mocks to verify parameter passing and method calling"""
    # Setup the mock
    mock_instance = MagicMock()
    mock_ontology_loader.return_value = mock_instance
    
    # Call the function
    result = load_ontology(op_context)
    
    # Verify the correct parameters were used to initialize OntologyLoaderController
    mock_ontology_loader.assert_called_once_with(
        source_ontology="envo",
        output_directory=op_context.op_config["output_directory"],
        generate_reports=False
    )
    
    # Verify that run_ontology_loader was called
    mock_instance.run_ontology_loader.assert_called_once()
    
    # The function doesn't have a return value
    assert result is None


# Method 2: Integration test approach similar to test_materialize_alldocs
# Uncomment this test when ready to run a full integration test
@pytest.mark.skipif(not os.getenv("MONGO_HOST"), reason="MongoDB host not set")
def test_load_ontology_integration(op_context):
    """Tests the load_ontology op with actual MongoDB connection and verifies results"""
    # Get MongoDB client
    mdb = op_context.resources.mongo.db
    
    # Check if ontology collections exist before running
    ontology_class_set_before = mdb.get_collection("ontology_class_set").count_documents({})
    ontology_relation_set_before = mdb.get_collection("ontology_relation_set").count_documents({})
    
    # Execute the op
    result = load_ontology(op_context)
    
    # Verify the op behavior:
    # 1. Check that ontology_class_set has entries
    ontology_class_count = mdb.get_collection("ontology_class_set").count_documents({})
    assert ontology_class_count > ontology_class_set_before
    
    # 2. Check that ontology_relation_set has entries
    ontology_relation_count = mdb.get_collection("ontology_relation_set").count_documents({})
    assert ontology_relation_count > ontology_relation_set_before
    
    # 3. Check for some known ENVO terms
    sample_envo_id = "ENVO:00000001"  # Example ENVO ID
    envo_term = mdb.get_collection("ontology_class_set").find_one({"id": sample_envo_id})
    assert envo_term is not None
    
    # 4. Check report files if generate_reports was True
    if op_context.op_config["generate_reports"]:
        output_dir = op_context.op_config["output_directory"]
        assert os.path.exists(os.path.join(output_dir, "ontology_inserts.tsv"))
        assert os.path.exists(os.path.join(output_dir, "ontology_updates.tsv"))
    
    # 5. Verify the function has no return value (was incorrectly expected to be 0)
    assert result is None
