# Note: This module's name starts with `manual_test_` instead of `test_` to signify that its author (currently) expects
#       people to run it manually as opposed to via automated test infrastructure. Its author chose that route after a
#       GitHub Actions workflow that runs tests in `test_` modules kept failing due to a database access issue.

from typing import Optional
import unittest
import re
import os
from datetime import datetime, timedelta

from pymongo import MongoClient, timeout
from pymongo.database import Database
from nmdc_schema.migrators.migrator_base import MigratorBase

from demo.metadata_migration.notebooks.bookkeeper import Bookkeeper, MigrationEvent

# Consume environment variables.
MONGO_HOST: str = os.getenv("MONGO_HOST", "localhost")
MONGO_USER: Optional[str] = os.getenv("MONGO_USERNAME", None)
MONGO_PASS: Optional[str] = os.getenv("MONGO_PASSWORD", None)
MONGO_DATABASE_NAME: str = os.getenv("MONGO_TEST_DBNAME", "test-migration-bookkeeper")

MONGO_TIMEOUT_DURATION: int = 3  # seconds


class FakeMigrator(MigratorBase):
    _to_version = "A.B.C"
    _from_version = "X.Y.Z"

    def upgrade(self):
        pass


class TestBookkeeper(unittest.TestCase):
    r"""
    Tests targeting the `Bookkeeper` class.

    You can format this file like this:
    $ python -m black demo/metadata_migration/notebooks/manual_test_bookkeeper.py

    You can start up a containerized MongoDB server like this:
    $ docker run --rm --detach --name mongo-test-migration-bookkeeper -p 27017:27017 mongo

    One that's running, other containers will be able to access it via:
    - host.docker.internal:27017

    You can run these tests like this:
    $ python -m unittest -v demo/metadata_migration/notebooks/manual_test_bookkeeper.py

    Reference: https://docs.python.org/3/library/unittest.html#basic-example
    """

    mongo_client: Optional[MongoClient] = None
    db: Optional[Database] = None

    def setUp(self) -> None:
        r"""
        Connects to the MongoDB server and gets a reference to the database.

        Note: This function runs before each test starts.
        """

        # Connect to the MongoDB server and store a reference to the connection.
        self.mongo_client = MongoClient(
            host=MONGO_HOST,
            username=MONGO_USER,
            password=MONGO_PASS,
        )
        with timeout(MONGO_TIMEOUT_DURATION):
            # Try connecting to the database server.
            _ = self.mongo_client.server_info()
            db = self.mongo_client[MONGO_DATABASE_NAME]

            # Ensure the database contains no collections.
            if len(db.list_collection_names()):
                raise KeyError(f"Database is not empty: {MONGO_DATABASE_NAME}")

        # Store a reference to the database.
        self.db = db

    def tearDown(self) -> None:
        r"""
        Drops all collections in the database and closes the connection to the MongoDB server.

        Note: This function runs after each test finishes.
        """

        # Drop all collections in the database.
        for collection_name in self.db.list_collection_names():
            self.db.drop_collection(collection_name)

        # Close the connection to the server.
        self.mongo_client.close()

    def test_get_current_timestamp(self):
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}$"
        ts_str = Bookkeeper.get_current_timestamp()

        # Verify the timestamp is a string having a valid format.
        self.assertIsInstance(ts_str, str)
        self.assertTrue(re.match(pattern, ts_str))

        # Verify the moment represented by the timestamp was within the past minute.
        ts = datetime.fromisoformat(ts_str)
        now_ts = datetime.now()
        time_difference = now_ts - ts
        self.assertLess(time_difference, timedelta(minutes=1))

    def test_init_method(self):
        # Confirm the view does not exist yet.
        view_name = "test_view"
        self.assertFalse(view_name in self.db.list_collection_names())

        # Instantiate the class-under-test.
        _ = Bookkeeper(
            mongo_client=self.mongo_client,
            database_name=MONGO_DATABASE_NAME,
            view_name=view_name,
        )

        # Confirm the view exists now.
        self.assertTrue(view_name in self.db.list_collection_names())

    def test_record_migration_event(self):
        # Instantiate a bookkeeper.
        bk = Bookkeeper(
            mongo_client=self.mongo_client,
            database_name=MONGO_DATABASE_NAME,
        )

        # Verify the collection is empty.
        collection = self.db.get_collection(bk.collection_name)
        self.assertTrue(collection.count_documents({}) == 0)

        # Record a "migration started" event.
        migrator = FakeMigrator()
        bk.record_migration_event(
            migrator=migrator, event=MigrationEvent.MIGRATION_STARTED
        )

        # Verify the migration event was recorded.
        self.assertTrue(collection.count_documents({}) == 1)
        doc = collection.find({})[0]
        self.assertIsInstance(doc["created_at"], str)
        self.assertIsInstance(doc["event"], str)
        self.assertEqual(doc["event"], MigrationEvent.MIGRATION_STARTED)
        self.assertIsInstance(doc["from_schema_version"], str)
        self.assertEqual(doc["from_schema_version"], migrator.get_origin_version())
        self.assertIsInstance(doc["to_schema_version"], str)
        self.assertEqual(doc["to_schema_version"], migrator.get_destination_version())
        self.assertIsInstance(doc["migrator_module"], str)

        # Verify the document in the view says the schema version is `null`.
        # Note: That's what I expect, since no "migration complete" events have been recorded yet.
        view = self.db.get_collection(bk.view_name)
        self.assertTrue(view.count_documents({}) == 1)
        view_doc = view.find({})[0]
        self.assertIsNone(view_doc["schema_version"])

        # Record a "migration completed" event.
        bk.record_migration_event(
            migrator=migrator, event=MigrationEvent.MIGRATION_COMPLETED
        )

        # Verify the migration event was recorded.
        self.assertTrue(collection.count_documents({}) == 2)

        # Verify the document in the view says the schema version matches the one recorded.
        view = self.db.get_collection(bk.view_name)
        self.assertTrue(view.count_documents({}) == 1)
        view_doc = view.find({})[0]
        self.assertEqual(view_doc["schema_version"], migrator.get_destination_version())

        # Finally, record another "migration started" event.
        bk.record_migration_event(
            migrator=migrator, event=MigrationEvent.MIGRATION_STARTED
        )

        # Confirm the document in the view once again says the schema version is `null`.
        self.assertTrue(view.count_documents({}) == 1)
        view_doc = view.find({})[0]
        self.assertIsNone(view_doc["schema_version"])


if __name__ == "__main__":
    unittest.main()
