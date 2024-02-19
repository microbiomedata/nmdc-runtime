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


class MigrationStatus(StrEnum):
    r"""
    Enumeration of all possible migration statuses that can be recorded.
    Reference: https://docs.python.org/3/library/enum.html#enum.StrEnum
    """
    IN_PROGRESS = 'IN_PROGRESS'
    DONE = 'DONE'


class Bookkeeper:
    def __init__(self, mongo_client: MongoClient):
        r"""Initialize a bookkeeper that can use the specified Mongo client."""
        self.database = "nmdc"
        self.collection_name = "_migration_history"
        self.view_name = "_migration_current_schema_version"

        # Store references to the database and collection.
        self.db = mongo_client[self.database]
        self.collection = self.db.get_collection(self.collection_name)

    @staticmethod
    def get_current_timestamp() -> str:
        r"""Returns an ISO 8601 timestamp (string) representing the current time in UTC."""
        utc_now = datetime.utcnow()
        iso_utc_now = utc_now.isoformat()
        return iso_utc_now

    def record_migration_status(self, migrator: MigratorBase, migration_status: MigrationStatus):
        r"""Records a migration status (along with some metadata) in the collection."""
        document = dict(
            from_schema_version=migrator.get_origin_version(),
            to_schema_version=migrator.get_destination_version(),
            migrator_module=migrator.__module__,  # name of the Python module in which the `Migrator` class is defined
            migration_status=migration_status,
            recorded_at=self.get_current_timestamp(),
        )
        self.collection.insert_one(document)

    def ensure_current_schema_version_view_exists(self):
        r"""
        Ensures the MongoDB view that indicates the current schema version, exists.

        References:
        - https://www.mongodb.com/community/forums/t/is-there-anyway-to-create-view-using-python/161363/2
        - https://www.mongodb.com/docs/manual/reference/method/db.createView/#mongodb-method-db.createView
        - https://pymongo.readthedocs.io/en/stable/api/pymongo/database.html#pymongo.database.Database.create_collection
        """
        if self.view_name not in self.db.list_collection_names():  # also lists views
            agg_pipeline = [{'$sort': {'recorded_at': -1}}, {'$limit': 1}]  # get document having the latest timestamp
            self.db.create_collection(name=self.view_name,
                                      viewOn=self.collection_name,
                                      pipeline=agg_pipeline,
                                      check_exists=True,  # only create the view if it doesn't already exist
                                      comment="The most recent migration status record.")
