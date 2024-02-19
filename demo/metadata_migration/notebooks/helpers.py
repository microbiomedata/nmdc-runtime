from pathlib import Path
import re
from typing import Dict
from enum import StrEnum
from datetime import datetime

from dotenv import dotenv_values
import yaml
from pymongo import MongoClient
from nmdc_schema.migrators.migrator_base import MigratorBase


class Config:
    """Wrapper class for configuration values related to database migration."""

    def parse_and_validate_notebook_config_file(
        self, notebook_config_file_path: str
    ) -> Dict[str, str]:
        # Validate the notebook config file path.
        if not Path(notebook_config_file_path).is_file():
            raise FileNotFoundError(
                f"Config file not found at: {notebook_config_file_path}"
            )

        # Parse the notebook config file.
        notebook_config = dotenv_values(notebook_config_file_path)

        # Validate the Mongo config file paths.
        origin_mongo_config_file_path = notebook_config[
            "PATH_TO_ORIGIN_MONGO_CONFIG_FILE"
        ]
        transformer_mongo_config_file_path = notebook_config[
            "PATH_TO_TRANSFORMER_MONGO_CONFIG_FILE"
        ]
        if not Path(origin_mongo_config_file_path).is_file():
            raise FileNotFoundError(
                f"Origin Mongo config file not found at: {origin_mongo_config_file_path}"
            )
        if not Path(transformer_mongo_config_file_path).is_file():
            raise FileNotFoundError(
                f"Transformer Mongo config file not found at: {transformer_mongo_config_file_path}"
            )

        # Validate the dump folder paths.
        origin_dump_folder_path = notebook_config["PATH_TO_ORIGIN_MONGO_DUMP_FOLDER"]
        transformer_dump_folder_path = notebook_config[
            "PATH_TO_TRANSFORMER_MONGO_DUMP_FOLDER"
        ]
        if not Path(origin_dump_folder_path).parent.is_dir():
            raise FileNotFoundError(
                f"Parent folder of {origin_dump_folder_path} (origin Mongo dump folder path) not found."
            )
        if not Path(transformer_dump_folder_path).parent.is_dir():
            raise FileNotFoundError(
                f"Parent folder of {transformer_dump_folder_path} (transformer Mongo dump folder path) not found."
            )

        # Validate the binary paths.
        mongodump_path = notebook_config["PATH_TO_MONGODUMP_BINARY"]
        mongorestore_path = notebook_config["PATH_TO_MONGORESTORE_BINARY"]
        if not Path(mongodump_path).is_file():
            raise FileNotFoundError(f"mongodump binary not found at: {mongodump_path}")
        if not Path(mongorestore_path).is_file():
            raise FileNotFoundError(
                f"mongorestore binary not found at: {mongorestore_path}"
            )

        return dict(
            origin_mongo_config_file_path=origin_mongo_config_file_path,
            transformer_mongo_config_file_path=transformer_mongo_config_file_path,
            origin_dump_folder_path=origin_dump_folder_path,
            transformer_dump_folder_path=transformer_dump_folder_path,
            mongodump_path=mongodump_path,
            mongorestore_path=mongorestore_path,
        )

    def parse_and_validate_mongo_config_file(
        self, mongo_config_file_path: str
    ) -> Dict[str, str]:
        # Parse the Mongo config files as YAML.
        with open(mongo_config_file_path, "r") as file:
            mongo_config = yaml.safe_load(file)

        # Validate the connection string.
        uri = mongo_config["uri"]
        if not re.match(
            r"^mongodb:\/\/.*", uri
        ):  # note: this is a sanity test, not a comprehensive test
            raise ValueError(f"uri value in {mongo_config_file_path} is invalid.")

        return dict(uri=uri)

    def __init__(self, notebook_config_file_path: str = "./.notebook.env") -> None:
        # Parse and validate the notebook config file.
        notebook_config = self.parse_and_validate_notebook_config_file(
            notebook_config_file_path
        )
        self.mongodump_path = notebook_config["mongodump_path"]
        self.mongorestore_path = notebook_config["mongorestore_path"]
        self.origin_dump_folder_path = notebook_config["origin_dump_folder_path"]
        self.transformer_dump_folder_path = notebook_config[
            "transformer_dump_folder_path"
        ]

        # Parse and validate the Mongo config files.
        self.origin_mongo_config_file_path = notebook_config[
            "origin_mongo_config_file_path"
        ]
        self.transformer_mongo_config_file_path = notebook_config[
            "transformer_mongo_config_file_path"
        ]
        origin_mongo_server_config = self.parse_and_validate_mongo_config_file(
            self.origin_mongo_config_file_path
        )
        transformer_mongo_server_config = self.parse_and_validate_mongo_config_file(
            self.transformer_mongo_config_file_path
        )
        self.origin_mongo_server_uri = origin_mongo_server_config["uri"]
        self.transformer_mongo_server_uri = transformer_mongo_server_config["uri"]


class MigrationEvent(StrEnum):
    r"""
    Enumeration of all migration events that can be recorded.
    Reference: https://docs.python.org/3/library/enum.html#enum.StrEnum
    """
    MIGRATION_STARTED = 'MIGRATION_STARTED'
    MIGRATION_COMPLETED = 'MIGRATION_COMPLETED'


class Bookkeeper:
    def __init__(
            self,
            mongo_client: MongoClient,
            database_name: str = "nmdc",
            collection_name: str = "_migration_log",
            view_name: str = "_migration_latest_schema_version",
    ):
        r"""Initialize a bookkeeper, which you can use to record migration-related events in a MongoDB database."""
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
        return iso_utc_now

    def record_migration_event(self, migrator: MigratorBase, to_schema_version: str, event: MigrationEvent) -> None:
        r"""
        Records a migration event in the collection.

        The `to_schema_version` parameter is independent of the `migrator` parameter because, even though the migrator
        does have a `.get_destination_version()` method, the string returned by that method is the one defined when the
        migrator was _written_, which is sometimes more generic than the one used for data validation when the migrator
        is _run_ (e.g. "1.2" as opposed to "1.2.3"). I think the latter value will make more sense to consumers.
        """
        document = dict(
            created_at=self.get_current_timestamp(),
            event=event,
            from_schema_version=migrator.get_origin_version(),
            to_schema_version=to_schema_version,
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
        if self.view_name not in self.db.list_collection_names():  # returns list of names of both collections and views
            agg_pipeline = [
                # Sort the documents so the most recent one is first.
                {'$sort': {'created_at': -1}},
                # Only preserve the first document.
                {'$limit': 1},
                # Derive a simplified document for the view.
                # Examples: `{ "schema_version": "1.2.3" }` or `{ "schema_version": null }`
                {
                    '$project': {
                        '_id': 0,  # omit the `_id` field
                        'schema_version': {  # add this field based upon the migration status
                            '$cond': {
                                'if': {'$eq': ['$event', MigrationEvent.MIGRATION_COMPLETED]},
                                'then': '$to_schema_version',  # database conforms to this version of the NMDC Schema
                                'else': None,  # database doesn't necessarily conform to any version of the NMDC Schema
                            }
                        },
                    }
                }
            ]
            self.db.create_collection(name=self.view_name,
                                      viewOn=self.collection_name,
                                      pipeline=agg_pipeline,
                                      check_exists=True,  # only create the view if it doesn't already exist
                                      comment="The version of the NMDC Schema to which the database conforms.")
