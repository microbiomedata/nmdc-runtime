from pathlib import Path

from dotenv import dotenv_values
import yaml

# Define input file paths.
notebook_config_file_path = "./.notebook.env"
origin_mongo_config_file_path = "./.mongo.origin.yaml"
transformer_mongo_config_file_path = "./.mongo.transformer.yaml"

# Define output file paths.
origin_dump_folder_path = "./mongodump.origin.out"
transformer_dump_folder_path = "./mongodump.transformer.out"

# Validate the config file path.
if not Path(notebook_config_file_path).is_file():
    raise FileNotFoundError(f"Config file not found at: {notebook_config_file_path}")

# Parse the notebook config file.
notebook_config = dotenv_values(notebook_config_file_path)
mongodump: str = notebook_config["PATH_TO_MONGODUMP_BINARY"]
mongorestore: str = notebook_config["PATH_TO_MONGORESTORE_BINARY"]

# Validate the parsed binary paths.
if not Path(mongodump).is_file():
    raise FileNotFoundError(f"mongodump binary not found at: {mongodump}")
if not Path(mongorestore).is_file():
    raise FileNotFoundError(f"mongorestore binary not found at: {mongorestore}")

# Validate the Mongo config file paths.
if not Path(origin_mongo_config_file_path).is_file():
    raise FileNotFoundError(f"Origin Mongo config file not found at: {origin_mongo_config_file_path}")
if not Path(transformer_mongo_config_file_path).is_file():
    raise FileNotFoundError(f"Transformer Mongo config file not found at: {transformer_mongo_config_file_path}")

# Parse the Mongo config files as YAML.
with open(origin_mongo_config_file_path, 'r') as file:
    origin_mongo_config = yaml.safe_load(file)
with open(transformer_mongo_config_file_path, 'r') as file:
    transformer_mongo_config = yaml.safe_load(file)

class Config:
    """Wrapper class for configuration values related to database migration."""
    notebook_config_file_path = notebook_config_file_path
    origin_mongo_config_file_path = origin_mongo_config_file_path
    transformer_mongo_config_file_path = transformer_mongo_config_file_path
    origin_dump_folder_path = origin_dump_folder_path
    transformer_dump_folder_path = transformer_dump_folder_path
    mongodump = mongodump
    mongorestore = mongorestore
    origin_mongo_server_uri = origin_mongo_config["uri"]
    transformer_mongo_server_uri = transformer_mongo_config["uri"]
