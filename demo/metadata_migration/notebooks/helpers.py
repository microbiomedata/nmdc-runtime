from pathlib import Path
from typing import Dict, Optional
import logging
from datetime import datetime

from dotenv import dotenv_values


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

        # Validate the dump folder paths.
        origin_dump_folder_path = notebook_config["PATH_TO_ORIGIN_MONGO_DUMP_FOLDER"]
        transformer_dump_folder_path = notebook_config["PATH_TO_TRANSFORMER_MONGO_DUMP_FOLDER"]
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
            raise FileNotFoundError(f"mongorestore binary not found at: {mongorestore_path}")
        if not Path(mongosh_path).is_file():
            raise FileNotFoundError(f"mongosh binary not found at: {mongosh_path}")

        origin_mongo_host = notebook_config["ORIGIN_MONGO_HOST"]
        origin_mongo_port = notebook_config["ORIGIN_MONGO_PORT"]
        origin_mongo_username = notebook_config["ORIGIN_MONGO_USERNAME"]
        origin_mongo_password = notebook_config["ORIGIN_MONGO_PASSWORD"]

        transformer_mongo_host = notebook_config["TRANSFORMER_MONGO_HOST"]
        transformer_mongo_port = notebook_config["TRANSFORMER_MONGO_PORT"]
        transformer_mongo_username = notebook_config["TRANSFORMER_MONGO_USERNAME"]
        transformer_mongo_password = notebook_config["TRANSFORMER_MONGO_PASSWORD"]

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
            transformer_mongo_host=transformer_mongo_host,
            transformer_mongo_port=transformer_mongo_port,
            transformer_mongo_username=transformer_mongo_username,
            transformer_mongo_password=transformer_mongo_password,
        )

    def __init__(self, notebook_config_file_path: str = "./.notebook.env") -> None:
        # Parse and validate the notebook config file.
        notebook_config = self.parse_and_validate_notebook_config_file(notebook_config_file_path)
        self.mongodump_path = notebook_config["mongodump_path"]
        self.mongorestore_path = notebook_config["mongorestore_path"]
        self.mongosh_path = notebook_config["mongosh_path"]
        self.origin_dump_folder_path = notebook_config["origin_dump_folder_path"]
        self.transformer_dump_folder_path = notebook_config["transformer_dump_folder_path"]

        # Parse the Mongo connection parameters.
        self.origin_mongo_host = notebook_config["origin_mongo_host"]
        self.origin_mongo_port = notebook_config["origin_mongo_port"]
        self.origin_mongo_username = notebook_config["origin_mongo_username"]
        self.origin_mongo_password = notebook_config["origin_mongo_password"]
        self.transformer_mongo_host = notebook_config["transformer_mongo_host"]
        self.transformer_mongo_port = notebook_config["transformer_mongo_port"]
        self.transformer_mongo_username = notebook_config["transformer_mongo_username"]
        self.transformer_mongo_password = notebook_config["transformer_mongo_password"]


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
