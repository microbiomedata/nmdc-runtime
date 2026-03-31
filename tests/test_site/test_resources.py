"""
Tests targeting code in `nmdc_runtime/site/resources.py`.
"""

from pymongo.database import Database

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.site.resources import MongoDB as MongoDBResource
from tests.lib.faker import Faker

class TestMongoDBAddDocs:
    def test_it_sets_add_date_when_inserting_study(self, op_context):
        db: Database = get_mongo_db()

        # Get an instance of the class containing the method-under-test.
        # Note: This access pattern was established in `conftest.py`.
        mongo_resource: MongoDBResource = op_context.resources.mongo

        # Get an `id` value that is not currently in use in the collection.
        study_a_id = "nmdc:sty-00-distinct"
        assert db["study_set"].count_documents({"id": study_a_id}) == 0

        # Get a reference to the collection into which we'll insert the document(s).
        study_set = db["study_set"]

        # Generate an `nmdc:Database` to pass to the method-under-test.
        faker = Faker()
        study_a = faker.generate_studies(1, id=study_a_id)[0]
        assert "provenance_metadata" not in study_a  # before
        nmdc_database = {"study_set": [study_a]}

        try:
            # Invoke the method-under-test.
            mongo_resource.add_docs(nmdc_database)

            # Assert that the inserted document has a `provenance_metadata.add_date` value
            # and lacks a `provenance_metadata.mod_date` field.
            inserted_doc = study_set.find_one({"id": study_a_id})
            assert inserted_doc is not None
            assert "provenance_metadata" in inserted_doc  # after
            assert "add_date" in inserted_doc["provenance_metadata"]
            assert isinstance(inserted_doc["provenance_metadata"]["add_date"], str)
            assert "mod_date" not in inserted_doc["provenance_metadata"]
        finally:
            # 🧹 Clean up.
            study_set.delete_many({"id": {"$in": [study_a_id]}})
            mongo_resource.client.close()

    def test_it_sets_mod_date_when_replacing_study(self, op_context):
        db: Database = get_mongo_db()

        # Get an instance of the class containing the method-under-test.
        # Note: This access pattern was established in `conftest.py`.
        mongo_resource: MongoDBResource = op_context.resources.mongo

        # Get an `id` value that is not currently in use in the collection.
        study_a_id = "nmdc:sty-00-distinct"
        assert db["study_set"].count_documents({"id": study_a_id}) == 0

        # Get a reference to the collection into which we'll insert the document(s).
        study_set = db["study_set"]

        # Generate an `nmdc:Database` to pass to the method-under-test.
        faker = Faker()
        study_a = faker.generate_studies(1, id=study_a_id)[0]
        assert "provenance_metadata" not in study_a  # before
        nmdc_database = {"study_set": [study_a]}

        try:

            # Insert the document into the database so that, later, when we use the method under
            # test, our operation is an update (i.e., replacement) rather than an insert.
            study_set.insert_one(study_a)

            # Modify the document (and strip its `_id`) before we submit it to the method under test.
            study_a.pop("_id")
            study_a["description"] = "NEW DESCRIPTION"

            # Invoke the method-under-test.
            mongo_resource.add_docs(nmdc_database)

            # Assert that the inserted document has a `provenance_metadata.mod_date` value
            # and lacks a `provenance_metadata.add_date` field.
            inserted_doc = study_set.find_one({"id": study_a_id})
            assert inserted_doc is not None
            assert "provenance_metadata" in inserted_doc  # after
            assert "mod_date" in inserted_doc["provenance_metadata"]
            assert isinstance(inserted_doc["provenance_metadata"]["mod_date"], str)
            assert "add_date" not in inserted_doc["provenance_metadata"]
        finally:
            # 🧹 Clean up.
            study_set.delete_many({"id": {"$in": [study_a_id]}})
            mongo_resource.client.close()

    def test_it_preserves_add_date_when_setting_mod_date(self, op_context):
        db: Database = get_mongo_db()

        # Get an instance of the class containing the method-under-test.
        # Note: This access pattern was established in `conftest.py`.
        mongo_resource: MongoDBResource = op_context.resources.mongo

        # Get an `id` value that is not currently in use in the collection.
        study_a_id = "nmdc:sty-00-distinct"
        assert db["study_set"].count_documents({"id": study_a_id}) == 0

        # Get a reference to the collection into which we'll insert the document(s).
        study_set = db["study_set"]

        # Generate an `nmdc:Database` to pass to the method-under-test.
        faker = Faker()
        study_a = faker.generate_studies(1, id=study_a_id)[0]
        original_add_date = "1990-01-23T01:23:45Z"
        study_a["provenance_metadata"] = {
            "type": "nmdc:ProvenanceMetadata",
            "add_date": original_add_date,
        }
        nmdc_database = {"study_set": [study_a]}

        try:
            # Insert the document into the database so that, later, when we use the method under
            # test, our operation is an update (i.e., replacement) rather than an insert.
            study_set.insert_one(study_a)

            # Modify the document (and strip its `_id`) before we submit it to the method under test.
            study_a.pop("_id")
            study_a["description"] = "NEW DESCRIPTION"

            # Invoke the method-under-test.
            mongo_resource.add_docs(nmdc_database)

            # Assert that the inserted document has a `provenance_metadata.mod_date` value
            # and has the original document's `provenance_metadata.add_date` value.
            inserted_doc = study_set.find_one({"id": study_a_id})
            assert inserted_doc is not None
            assert "provenance_metadata" in inserted_doc  # after
            assert "mod_date" in inserted_doc["provenance_metadata"]
            assert isinstance(inserted_doc["provenance_metadata"]["mod_date"], str)
            assert "add_date" in inserted_doc["provenance_metadata"]
            assert inserted_doc["provenance_metadata"]["add_date"] == original_add_date
        finally:
            # 🧹 Clean up.
            study_set.delete_many({"id": {"$in": [study_a_id]}})
            mongo_resource.client.close()
