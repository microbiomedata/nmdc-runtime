from pathlib import Path
import re
from typing import Dict

from dotenv import dotenv_values
import yaml


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
