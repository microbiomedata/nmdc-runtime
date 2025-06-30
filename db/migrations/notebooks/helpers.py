from pathlib import Path
from typing import Dict, Optional, List
import logging
from datetime import datetime
from functools import cache

from dotenv import dotenv_values
from linkml_runtime import SchemaView


DATABASE_CLASS_NAME = "Database"


class Config:
    """Wrapper class for configuration values related to database migration."""

    @staticmethod
    def make_mongo_cli_base_options(
        mongo_host: str,
        mongo_port: str,
        mongo_username: Optional[str],
        mongo_password: Optional[str],
    ) -> str:
        r"""Compile common Mongo CLI options into a reusable string."""

        # Always include the CLI options that specify the server host and port.
        base_options: List[str] = [
            f"--host='{mongo_host}'",
            f"--port='{mongo_port}'",
        ]

        # If the server uses authentication, include authentication-related CLI options.
        server_uses_authentication: bool = mongo_username not in ["", None]
        if server_uses_authentication:
            base_options.extend(
                [
                    f"--username='{mongo_username}'",
                    f"--password='{mongo_password}'",
                    f"--authenticationDatabase='admin'",
                ]
            )

        return " ".join(base_options)

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
        mongosh_path = notebook_config["PATH_TO_MONGOSH_BINARY"]
        if not Path(mongodump_path).is_file():
            raise FileNotFoundError(f"mongodump binary not found at: {mongodump_path}")
        if not Path(mongorestore_path).is_file():
            raise FileNotFoundError(
                f"mongorestore binary not found at: {mongorestore_path}"
            )
        if not Path(mongosh_path).is_file():
            raise FileNotFoundError(f"mongosh binary not found at: {mongosh_path}")

        origin_mongo_host = notebook_config["ORIGIN_MONGO_HOST"]
        origin_mongo_port = notebook_config["ORIGIN_MONGO_PORT"]
        origin_mongo_username = notebook_config["ORIGIN_MONGO_USERNAME"]
        origin_mongo_password = notebook_config["ORIGIN_MONGO_PASSWORD"]
        origin_mongo_database_name = notebook_config["ORIGIN_MONGO_DATABASE_NAME"]

        transformer_mongo_host = notebook_config["TRANSFORMER_MONGO_HOST"]
        transformer_mongo_port = notebook_config["TRANSFORMER_MONGO_PORT"]
        transformer_mongo_username = notebook_config["TRANSFORMER_MONGO_USERNAME"]
        transformer_mongo_password = notebook_config["TRANSFORMER_MONGO_PASSWORD"]
        transformer_mongo_database_name = notebook_config[
            "TRANSFORMER_MONGO_DATABASE_NAME"
        ]

        # Validate the database names.
        if origin_mongo_database_name.strip() == "":
            raise ValueError(f"Origin database name cannot be empty")
        if transformer_mongo_database_name.strip() == "":
            raise ValueError(f"Transformer database name cannot be empty")
        if all(
            [
                origin_mongo_host == transformer_mongo_host,
                origin_mongo_port == transformer_mongo_port,
                origin_mongo_database_name == transformer_mongo_database_name,
            ]
        ):
            # Note: We don't allow the use of the origin database as the transformer,
            #       because that would prevent us from easily aborting the migration.
            raise ValueError(
                f"The origin and transformer cannot both be the same database"
            )

        return dict(
            origin_dump_folder_path=origin_dump_folder_path,
            transformer_dump_folder_path=transformer_dump_folder_path,
            mongodump_path=mongodump_path,
            mongorestore_path=mongorestore_path,
            mongosh_path=mongosh_path,
            origin_mongo_host=origin_mongo_host,
            origin_mongo_port=origin_mongo_port,
            origin_mongo_username=origin_mongo_username,
            origin_mongo_password=origin_mongo_password,
            origin_mongo_database_name=origin_mongo_database_name,
            transformer_mongo_host=transformer_mongo_host,
            transformer_mongo_port=transformer_mongo_port,
            transformer_mongo_username=transformer_mongo_username,
            transformer_mongo_password=transformer_mongo_password,
            transformer_mongo_database_name=transformer_mongo_database_name,
        )

    def __init__(self, notebook_config_file_path: str = "./.notebook.env") -> None:
        # Parse and validate the notebook config file.
        notebook_config = self.parse_and_validate_notebook_config_file(
            notebook_config_file_path
        )
        self.mongodump_path = notebook_config["mongodump_path"]
        self.mongorestore_path = notebook_config["mongorestore_path"]
        self.mongosh_path = notebook_config["mongosh_path"]
        self.origin_dump_folder_path = notebook_config["origin_dump_folder_path"]
        self.transformer_dump_folder_path = notebook_config[
            "transformer_dump_folder_path"
        ]

        # Parse the Mongo connection parameters.
        self.origin_mongo_host = notebook_config["origin_mongo_host"]
        self.origin_mongo_port = notebook_config["origin_mongo_port"]
        self.origin_mongo_username = notebook_config["origin_mongo_username"]
        self.origin_mongo_password = notebook_config["origin_mongo_password"]
        self.origin_mongo_database_name = notebook_config["origin_mongo_database_name"]
        self.transformer_mongo_host = notebook_config["transformer_mongo_host"]
        self.transformer_mongo_port = notebook_config["transformer_mongo_port"]
        self.transformer_mongo_username = notebook_config["transformer_mongo_username"]
        self.transformer_mongo_password = notebook_config["transformer_mongo_password"]
        self.transformer_mongo_database_name = notebook_config[
            "transformer_mongo_database_name"
        ]


def setup_logger(
    log_file_path: Optional[str] = None,
    logger_name: str = "migrator_logger",
    log_level: int = logging.DEBUG,
) -> logging.Logger:
    r"""
    Returns a logger that writes to a file at the specified log file path
    (default: "./{YYYYMMDD_HHMM}_migration.log").
    """

    # If no log file path was specified, generate one.
    if log_file_path is None:
        yyyymmdd_hhmm: str = datetime.now().strftime("%Y%m%d_%H%M")  # YYYYMMDD_HHMM
        log_file_path = f"./{yyyymmdd_hhmm}_migration.log"

    logger = logging.getLogger(name=logger_name)
    logger.setLevel(level=log_level)
    file_handler = logging.FileHandler(log_file_path)
    formatter = logging.Formatter(
        fmt="[%(asctime)s %(name)s %(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    if logger.hasHandlers():
        logger.handlers.clear()  # avoids duplicate log entries
    logger.addHandler(file_handler)
    return logger


def get_collection_names_from_schema(schema_view: SchemaView) -> List[str]:
    """
    Returns the names of the slots of the `Database` class that describe database collections.

    :param schema_view: A `SchemaView` instance

    Source: This function was copied from https://github.com/microbiomedata/refscan/blob/main/refscan/lib/helpers.py
            with permission from its author.
    """
    collection_names = []

    for slot_name in schema_view.class_slots(DATABASE_CLASS_NAME):
        slot_definition = schema_view.induced_slot(slot_name, DATABASE_CLASS_NAME)

        # Filter out any hypothetical (future) slots that don't correspond to a collection (e.g. `db_version`).
        if slot_definition.multivalued and slot_definition.inlined_as_list:
            collection_names.append(slot_name)

        # Filter out duplicate names. This is to work around the following issues in the schema:
        # - https://github.com/microbiomedata/nmdc-schema/issues/1954
        # - https://github.com/microbiomedata/nmdc-schema/issues/1955
        collection_names = list(set(collection_names))

    return collection_names


@cache  # memoizes the decorated function
def translate_class_uri_into_schema_class_name(
    schema_view: SchemaView, class_uri: str
) -> Optional[str]:
    r"""
    Returns the name of the schema class that has the specified value as its `class_uri`.

    Example: "nmdc:Biosample" (a `class_uri` value) -> "Biosample" (a class name)

    References:
    - https://linkml.io/linkml/developers/schemaview.html#linkml_runtime.utils.schemaview.SchemaView.all_classes
    - https://linkml.io/linkml/code/metamodel.html#linkml_runtime.linkml_model.meta.ClassDefinition.class_uri

    Source: This function was copied from https://github.com/microbiomedata/refscan/blob/main/refscan/lib/helpers.py
            with permission from its author.
    """
    schema_class_name = None
    all_class_definitions_in_schema = schema_view.all_classes()
    for class_name, class_definition in all_class_definitions_in_schema.items():
        if class_definition.class_uri == class_uri:
            schema_class_name = class_definition.name
            break
    return schema_class_name


def derive_schema_class_name_from_document(
    schema_view: SchemaView, document: dict
) -> Optional[str]:
    r"""
    Returns the name of the schema class, if any, of which the specified document claims to represent an instance.

    This function is written under the assumption that the document has a `type` field whose value is the `class_uri`
    belonging to the schema class of which the document represents an instance. Slot definition for such a field:
    https://github.com/microbiomedata/berkeley-schema-fy24/blob/fc2d9600/src/schema/basic_slots.yaml#L420-L436

    Source: This function was copied from https://github.com/microbiomedata/refscan/blob/main/refscan/lib/helpers.py
            with permission from its author.
    """
    schema_class_name = None
    if "type" in document and isinstance(document["type"], str):
        class_uri = document["type"]
        schema_class_name = translate_class_uri_into_schema_class_name(
            schema_view, class_uri
        )
    return schema_class_name
