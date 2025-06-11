import os
import pytest
from unittest.mock import patch, MagicMock
from dagster import build_op_context
from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.site.ops import load_ontology
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


@pytest.fixture
def client_config():
    # Print details about the MongoDB configuration for debugging
    mongo_host = os.getenv("MONGO_HOST")
    mongo_dbname = os.getenv("MONGO_DBNAME")
    mongo_username = os.getenv("MONGO_USERNAME")

    logging.info(f"Test MongoDB connection details:")
    logging.info(f"- MONGO_HOST: {mongo_host}")
    logging.info(f"- MONGO_DBNAME: {mongo_dbname}")
    logging.info(f"- MONGO_USERNAME: {mongo_username}")

    # For local development outside Docker, try connecting to the Docker-exposed port
    if mongo_host == "mongodb://mongo:27017":
        alternative_host = "mongodb://localhost:27018"
        logging.info(f"- Inside test: MongoDB host is set to container name. "
              f"If running test locally (not in Docker), try: {alternative_host}")

    return {
        "dbname": mongo_dbname,
        "host": mongo_host,
        "password": os.getenv("MONGO_PASSWORD"),
        "username": mongo_username,
    }


@pytest.fixture
def op_context(client_config, tmp_path):
    return build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)},
        op_config={
            "source_ontology": "envo",
            "output_directory": str(tmp_path),
            "generate_reports": False
        }
    )


# This test will always run - it doesn't require MongoDB connection
@patch('nmdc_runtime.site.ops.OntologyLoaderController')
def test_load_ontology(mock_ontology_loader, op_context):
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
        generate_reports=False,
        mongo_client=op_context.resources.mongo.client
    )

    # Verify that run_ontology_loader was called
    mock_instance.run_ontology_loader.assert_called_once()

    # The function doesn't have a return value
    assert result is None



@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_load_ontology_integration(op_context):
    """Tests the load_ontology op with actual MongoDB connection and verifies results"""

    # Get MongoDB client
    mdb = op_context.resources.mongo.db

    # Print detail about the MongoDB connection
    logging.info(f"Connected to MongoDB: {op_context.resources.mongo.db.client.address}")

    # Check if ontology collections exist before running
    ontology_class_set_before = mdb.get_collection("ontology_class_set").count_documents({})
    ontology_relation_set_before = mdb.get_collection("ontology_relation_set").count_documents({})

    logging.info(f"Before running: {ontology_class_set_before} classes, {ontology_relation_set_before} relations")

    # Execute the op
    result = load_ontology(op_context)

    # Verify the op behavior:
    # 1. Check that ontology_class_set has entries
    ontology_class_count = mdb.get_collection("ontology_class_set").count_documents({})
    logging.info(f"After running: {ontology_class_count} classes")

    # 2. Check that ontology_relation_set has entries
    ontology_relation_count = mdb.get_collection("ontology_relation_set").count_documents({})
    logging.info(f"After running: {ontology_relation_count} relations")

    # 3. Check for some known ENVO terms if we have ontology data
    assert ontology_class_count > 0
    assert ontology_relation_count > 0

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
