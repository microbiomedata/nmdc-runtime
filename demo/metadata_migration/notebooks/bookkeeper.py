from typing import Optional
from enum import Enum
from datetime import datetime

from pymongo import MongoClient
from nmdc_schema.migrators.migrator_base import MigratorBase


class MigrationEvent(str, Enum):
    r"""
    Enumeration of all migration events that can be recorded.
    Reference: https://docs.python.org/3.10/library/enum.html#others

    >>> MigrationEvent.MIGRATION_COMPLETED.value
    'MIGRATION_COMPLETED'
    >>> MigrationEvent.MIGRATION_STARTED.value
    'MIGRATION_STARTED'
    """

    MIGRATION_STARTED = "MIGRATION_STARTED"
    MIGRATION_COMPLETED = "MIGRATION_COMPLETED"


class Bookkeeper:
    r"""
    A class you can use to record migration-related events in a Mongo database.
    """

    def __init__(
        self,
        mongo_client: MongoClient,
        database_name: str = "nmdc",
        collection_name: str = "_migration_events",
        view_name: str = "_migration_latest_schema_version",
    ):
        self.database_name = database_name
        self.collection_name = collection_name
        self.view_name = view_name

        # Store references to the database and collection.
        self.db = mongo_client[self.database_name]
        self.collection = self.db.get_collection(self.collection_name)

        # Ensure the MongoDB view that indicates the current schema version, exists.
        self.ensure_current_schema_version_view_exists()

    @staticmethod
    def get_current_timestamp() -> str:
        r"""Returns an ISO 8601 timestamp (string) representing the current time in UTC."""
        utc_now = datetime.utcnow()
        iso_utc_now = utc_now.isoformat()
        return iso_utc_now  # e.g. "2024-02-21T04:31:03.115107"

    def record_migration_event(
        self,
        migrator: MigratorBase,
        event: MigrationEvent,
        to_schema_version: Optional[str] = None,
    ) -> None:
        r"""
        Records a migration event in the collection.

        The `to_schema_version` parameter is independent of the `migrator` parameter because, even though the migrator
        does have a `.get_destination_version()` method, the string returned by that method is the one defined when the
        migrator was _written_, which is sometimes more generic than the one used for data validation when the migrator
        is _run_ (e.g. "1.2" as opposed to "1.2.3"). So, this method provides a means by which the calling code can,
        optionally, specify a more precise version identifier.
        """

        # If a custom schema version identifier was specified, use that; otherwise, use the one built into the migrator.
        to_schema_version_str = migrator.get_destination_version()
        if to_schema_version is not None:
            to_schema_version_str = to_schema_version

        document = dict(
            created_at=self.get_current_timestamp(),
            event=event.value,
            from_schema_version=migrator.get_origin_version(),
            to_schema_version=to_schema_version_str,
            migrator_module=migrator.__module__,  # name of the Python module in which the `Migrator` class is defined
        )
        self.collection.insert_one(document)

    def ensure_current_schema_version_view_exists(self) -> None:
        r"""
        Ensures the MongoDB view that indicates the current schema version, exists.

        References:
        - https://www.mongodb.com/community/forums/t/is-there-anyway-to-create-view-using-python/161363/2
        - https://www.mongodb.com/docs/manual/reference/method/db.createView/#mongodb-method-db.createView
        - https://pymongo.readthedocs.io/en/stable/api/pymongo/database.html#pymongo.database.Database.create_collection
        """
        if (
            self.view_name not in self.db.list_collection_names()
        ):  # returns list of names of both collections and views
            agg_pipeline = [
                # Sort the documents so the most recent one is first.
                {"$sort": {"created_at": -1}},
                # Only preserve the first document.
                {"$limit": 1},
                # Derive a simplified document for the view.
                # Examples: `{ "schema_version": "1.2.3" }` or `{ "schema_version": null }`
                {
                    "$project": {
                        "_id": 0,  # omit the `_id` field
                        "schema_version": {  # add this field based upon the migration status
                            "$cond": {
                                "if": {
                                    "$eq": [
                                        "$event",
                                        MigrationEvent.MIGRATION_COMPLETED.value,
                                    ]
                                },
                                "then": "$to_schema_version",  # database conforms to this version of the NMDC Schema
                                "else": None,  # database doesn't necessarily conform to any version of the NMDC Schema
                            }
                        },
                    }
                },
            ]
            self.db.create_collection(
                name=self.view_name,
                viewOn=self.collection_name,
                pipeline=agg_pipeline,
                check_exists=True,  # only create the view if it doesn't already exist
                comment="The version of the NMDC Schema to which the database conforms.",
            )
