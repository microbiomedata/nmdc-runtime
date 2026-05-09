"""
Functions related to running `mongodump` and `mongorestore`.
"""

from pathlib import Path

from src.lib.config import DatabaseConfig


def build_mongodump_command(
    mongodump_path: Path,
    collection_name: str,
    database_config: DatabaseConfig,
    dump_folder_path: Path,
) -> list[str]:
    """
    Build a `mongodump` command that would dump a single MongoDB collection to a set of compressed files.

    :param mongodump_path: Path to the `mongodump` executable you want to use.
    :param collection_name: Name of the MongoDB collection you want to dump.
    :param database_config: `DatabaseConfig` describing the MongoDB database containing the collection.
    :param dump_folder_path: Path to the folder in which you want the compressed files to be stored.

    >>> cmd = build_mongodump_command(
    ...     mongodump_path=Path("/path/to/mongodump"),
    ...     collection_name="my_collection",
    ...     database_config=DatabaseConfig(
    ...         host="localhost",
    ...         port=27017,
    ...         name="my_database",
    ...         username="my_user",
    ...         password="my_password",
    ...         direct_connection=True,
    ...     ),
    ...     dump_folder_path=Path("/path/to/dump/folder"),
    ... )
    >>> cmd == [
    ...     '/path/to/mongodump',
    ...     '--collection', 'my_collection',
    ...     '--gzip',
    ...     '--out', '/path/to/dump/folder',
    ...     '--host', 'localhost',
    ...     '--port', '27017',
    ...     '--db', 'my_database',
    ...     '--username', 'my_user',
    ...     '--password', 'my_password',
    ...     '--authenticationDatabase', 'admin',
    ... ]
    True
    """

    shell_command_parts = [
        str(mongodump_path),
        "--collection",
        collection_name,
        "--gzip",
        "--out",
        str(dump_folder_path),
    ]
    shell_command_parts.extend(database_config.get_cli_options())
    return shell_command_parts


def build_mongorestore_command(
    mongorestore_path: Path,
    source_database_name: str,
    destination_database_name: str,
    destination_database_config: DatabaseConfig,
    dump_folder_path: Path,
) -> list[str]:
    """
    Build a `mongorestore` command that would restore MongoDB collections from a set of compressed files.

    :param mongorestore_path: Path to the `mongorestore` executable you want to use.
    :param source_database_name: Name of the database that was dumped by `mongodump` (this will be
                                 used as the `--nsFrom` argument to `mongorestore`).
    :param destination_database_name: Name of the database you want to restore to (this will be
                                      used as the `--nsTo` argument to `mongorestore`).
    :param destination_database_config: `DatabaseConfig` describing the MongoDB server into which
                                        you want to restore the collections. The database name in
                                        this `DatabaseConfig` will be ignored.
    :param dump_folder_path: Path to the folder where the compressed dump files reside.

    >>> cmd = build_mongorestore_command(
    ...     mongorestore_path=Path("/path/to/mongorestore"),
    ...     source_database_name="my_source_database",
    ...     destination_database_name="my_destination_database",
    ...     destination_database_config=DatabaseConfig(
    ...         host="localhost",
    ...         port=27017,
    ...         name="ignored_database_name",
    ...         username="my_user",
    ...         password="my_password",
    ...         direct_connection=True,
    ...     ),
    ...     dump_folder_path=Path("/path/to/dump/folder"),
    ... )
    >>> cmd == [
    ...     '/path/to/mongorestore',
    ...     '--nsFrom', 'my_source_database.*',
    ...     '--nsTo', 'my_destination_database.*',
    ...     '--drop',
    ...     '--stopOnError',
    ...     '--gzip',
    ...     '--dir', '/path/to/dump/folder',
    ...     '--host', 'localhost',
    ...     '--port', '27017',
    ...     '--username', 'my_user',
    ...     '--password', 'my_password',
    ...     '--authenticationDatabase', 'admin',
    ... ]
    True
    """

    shell_command_parts = [
        str(mongorestore_path),
        "--nsFrom",
        f"{source_database_name}.*",
        "--nsTo",
        f"{destination_database_name}.*",
        "--drop",
        "--stopOnError",
        "--gzip",
        "--dir",
        str(dump_folder_path),
    ]
    shell_command_parts.extend(destination_database_config.get_cli_options(include_db_option=False))
    return shell_command_parts
