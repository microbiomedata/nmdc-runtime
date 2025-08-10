import unittest
from tempfile import NamedTemporaryFile as TempFile, mkdtemp
import shutil

from db.migrations.notebooks.helpers import Config


class TestConfig(unittest.TestCase):
    r"""
    Tests targeting the `Config` class.

    You can format this file like this:
    $ python -m black db/migrations/notebooks/test_helpers.py

    You can run these tests like this:
    $ python -m unittest -v db/migrations/notebooks/test_helpers.py

    Reference: https://docs.python.org/3/library/unittest.html#basic-example
    """

    def test_make_mongo_cli_base_options(self):
        # Case 1: Input includes username.
        s = Config.make_mongo_cli_base_options(
            mongo_host="thehost",
            mongo_port="12345",
            mongo_username="alice",
            mongo_password="shhh",
        )
        assert (
            s
            == f"""
            --host='thehost' --port='12345' --username='alice' --password='shhh' --authenticationDatabase='admin'
        """.strip()
        )

        # Case 2: Input username is empty string.
        s = Config.make_mongo_cli_base_options(
            mongo_host="thehost",
            mongo_port="12345",
            mongo_username="",
            mongo_password="shhh",
        )
        assert (
            s
            == f"""
            --host='thehost' --port='12345'
        """.strip()
        )

        # Case 2: Input username is `None`.
        s = Config.make_mongo_cli_base_options(
            mongo_host="thehost",
            mongo_port="12345",
            mongo_username=None,
            mongo_password="shhh",
        )
        assert (
            s
            == f"""
            --host='thehost' --port='12345'
        """.strip()
        )

    def test_init_method(self):
        with (
            TempFile() as notebook_config_file,
            TempFile() as mongodump_binary,
            TempFile() as mongorestore_binary,
            TempFile() as mongosh_binary,
        ):

            # Create named temporary directories and get their paths.
            origin_dump_folder_path = mkdtemp()
            transformer_dump_folder_path = mkdtemp()

            # Define Mongo server connection parameters.
            origin_mongo_host = "origin"
            origin_mongo_port = "11111"
            origin_mongo_username = "origin_username"
            origin_mongo_password = "origin_password"
            transformer_mongo_host = "transformer"
            transformer_mongo_port = "22222"
            transformer_mongo_username = "transformer_username"
            transformer_mongo_password = "transformer_password"

            # Define Mongo database selection parameters.
            origin_mongo_database_name = "origin_database_name"
            transformer_mongo_database_name = "transformer_database_name"

            # Use familiar aliases in an attempt to facilitate writing the `assert` section below.
            mongodump_path = mongodump_binary.name
            mongorestore_path = mongorestore_binary.name
            mongosh_path = mongosh_binary.name

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
                ORIGIN_MONGO_DATABASE_NAME=origin_mongo_database_name,
                TRANSFORMER_MONGO_HOST=transformer_mongo_host,
                TRANSFORMER_MONGO_PORT=transformer_mongo_port,
                TRANSFORMER_MONGO_USERNAME=transformer_mongo_username,
                TRANSFORMER_MONGO_PASSWORD=transformer_mongo_password,
                TRANSFORMER_MONGO_DATABASE_NAME=transformer_mongo_database_name,
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
            assert cfg.origin_mongo_database_name == origin_mongo_database_name
            assert cfg.transformer_mongo_host == transformer_mongo_host
            assert cfg.transformer_mongo_port == transformer_mongo_port
            assert cfg.transformer_mongo_username == transformer_mongo_username
            assert cfg.transformer_mongo_password == transformer_mongo_password
            assert (
                cfg.transformer_mongo_database_name == transformer_mongo_database_name
            )

            # Delete the temporary directories (i.e. clean up).
            shutil.rmtree(origin_dump_folder_path)
            shutil.rmtree(transformer_dump_folder_path)


if __name__ == "__main__":
    unittest.main()
