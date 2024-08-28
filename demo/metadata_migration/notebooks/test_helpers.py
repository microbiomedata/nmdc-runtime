import unittest
from tempfile import NamedTemporaryFile as TempFile, mkdtemp
import shutil

from demo.metadata_migration.notebooks.helpers import Config


class TestConfig(unittest.TestCase):
    r"""
    Tests targeting the `Config` class.

    You can format this file like this:
    $ python -m black demo/metadata_migration/notebooks/test_helpers.py

    You can run these tests like this:
    $ python -m unittest -v demo/metadata_migration/notebooks/test_helpers.py

    Reference: https://docs.python.org/3/library/unittest.html#basic-example
    """

    def test_init_method(self):
        with (TempFile() as notebook_config_file, 
              TempFile() as origin_mongo_config_file, 
              TempFile() as transformer_mongo_config_file, 
              TempFile() as mongodump_binary, 
              TempFile() as mongorestore_binary, 
              TempFile() as mongosh_binary):

            # Create named temporary directories and get their paths.
            origin_dump_folder_path = mkdtemp()
            transformer_dump_folder_path = mkdtemp()

            # Populate the Mongo config files, then reset their file pointers.
            origin_mongo_server_uri = f"mongodb://u:p@origin:12345"
            transformer_mongo_server_uri = f"mongodb://u:p@transformer:12345"
            origin_mongo_host = "origin"
            origin_mongo_port = "11111"
            origin_mongo_username = "origin_username"
            origin_mongo_password = "origin_password"
            transformer_mongo_host = "transformer"
            transformer_mongo_port = "22222"
            transformer_mongo_username = "transformer_username"
            transformer_mongo_password = "transformer_password"            
            origin_mongo_yaml = f"uri: {origin_mongo_server_uri}\n"
            transformer_mongo_yaml = f"uri: {transformer_mongo_server_uri}\n"
            origin_mongo_config_file.write(origin_mongo_yaml.encode("utf-8"))
            transformer_mongo_config_file.write(transformer_mongo_yaml.encode("utf-8"))
            origin_mongo_config_file.seek(0)
            transformer_mongo_config_file.seek(0)

            # Use familiar aliases in an attempt to facilitate writing the `assert` section below.
            mongodump_path = mongodump_binary.name
            mongorestore_path = mongorestore_binary.name
            mongosh_path = mongosh_binary.name
            origin_mongo_config_file_path = origin_mongo_config_file.name
            transformer_mongo_config_file_path = transformer_mongo_config_file.name

            # Populate the notebook config file, then reset its file pointer.
            notebook_config_values = dict(
                PATH_TO_ORIGIN_MONGO_DUMP_FOLDER=origin_dump_folder_path,
                PATH_TO_TRANSFORMER_MONGO_DUMP_FOLDER=transformer_dump_folder_path,
                PATH_TO_MONGODUMP_BINARY=mongodump_path,
                PATH_TO_MONGORESTORE_BINARY=mongorestore_path,
                PATH_TO_MONGOSH_BINARY=mongosh_path,
                ORIGIN_MONGO_HOST=origin_mongo_host,
                ORIGIN_MONGO_PORT=origin_mongo_port,
                ORIGIN_MONGO_USERNAME=origin_mongo_username,
                ORIGIN_MONGO_PASSWORD=origin_mongo_password,
                TRANSFORMER_MONGO_HOST=transformer_mongo_host,
                TRANSFORMER_MONGO_PORT=transformer_mongo_port,
                TRANSFORMER_MONGO_USERNAME=transformer_mongo_username,
                TRANSFORMER_MONGO_PASSWORD=transformer_mongo_password,
            )
            for key, value in notebook_config_values.items():
                notebook_config_file.write(f"{key} = {value}\n".encode("utf-8"))
            notebook_config_file.seek(0)

            # Instantiate the class-under-test.
            cfg = Config(notebook_config_file.name)

            # Validate the instance.
            assert cfg.mongodump_path == mongodump_path
            assert cfg.mongorestore_path == mongorestore_path
            assert cfg.origin_dump_folder_path == origin_dump_folder_path
            assert cfg.transformer_dump_folder_path == transformer_dump_folder_path
            assert cfg.origin_mongo_host == origin_mongo_host
            assert cfg.origin_mongo_port == origin_mongo_port
            assert cfg.origin_mongo_username == origin_mongo_username
            assert cfg.origin_mongo_password == origin_mongo_password
            assert cfg.transformer_mongo_host == transformer_mongo_host
            assert cfg.transformer_mongo_port == transformer_mongo_port
            assert cfg.transformer_mongo_username == transformer_mongo_username
            assert cfg.transformer_mongo_password == transformer_mongo_password

            # Delete the temporary directories (i.e. clean up).
            shutil.rmtree(origin_dump_folder_path)
            shutil.rmtree(transformer_dump_folder_path)


if __name__ == "__main__":
    unittest.main()
