"""
Functions related to running `mongodump` and `mongorestore`.
"""

from pathlib import Path


def build_mongodump_command(
    mongodump_path: Path,
    collection_name: str,
    dump_folder_path: Path,
    database_cli_options: list[str],
) -> list[str]:
    """
    Build a `mongodump` command that would dump a single MongoDB collection to a set of compressed files.

    :param mongodump_path: Path to the `mongodump` executable you want to use.
    :param collection_name: Name of the MongoDB collection you want to dump.
    :param dump_folder_path: Path to the folder in which you want the compressed files to be stored.
    :param database_cli_options: mongodump CLI options for accessing the MongoDB database containing
                                 the collections you want to dump.

    >>> cmd = build_mongodump_command(
    ...     mongodump_path=Path("/path/to/mongodump"),
    ...     collection_name="my_collection",
    ...     dump_folder_path=Path("/path/to/dump/folder"),
    ...     database_cli_options=["--some-other-key", "some-other-value"],
    ... )
    >>> cmd == [
    ...     '/path/to/mongodump',
    ...     '--collection', 'my_collection',
    ...     '--gzip',
    ...     '--out', '/path/to/dump/folder',
    ...     '--some-other-key', 'some-other-value',
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
    shell_command_parts.extend(database_cli_options)
    return shell_command_parts


def build_mongorestore_command(
    mongorestore_path: Path,
    source_database_name: str,
    destination_database_name: str,
    dump_folder_path: Path,
    destination_server_cli_options: list[str],
    collection_names: list[str] | None = None,
) -> list[str]:
    """
    Build a `mongorestore` command that would restore MongoDB collections from a set of compressed files.

    :param mongorestore_path: Path to the `mongorestore` executable you want to use.
    :param source_database_name: Name of the database that was dumped by `mongodump` (this will be
                                 used as the `--nsFrom` argument to `mongorestore`).
    :param destination_database_name: Name of the database you want to restore to (this will be
                                      used as the `--nsTo` argument to `mongorestore`).
    :param dump_folder_path: Path to the folder where the compressed dump files reside.
    :param destination_server_cli_options: mongorestore CLI options for accessing the destination
                                           MongoDB server.
    :param collection_names: Names of collections to restore. When omitted, all collections in the
                             dump are restored.

    >>> cmd = build_mongorestore_command(
    ...     mongorestore_path=Path("/path/to/mongorestore"),
    ...     source_database_name="my_source_database",
    ...     destination_database_name="my_destination_database",
    ...     dump_folder_path=Path("/path/to/dump/folder"),
    ...     destination_server_cli_options=["--some-other-key", "some-other-value"],
    ...     collection_names=["my_collection"],
    ... )
    >>> cmd == [
    ...     '/path/to/mongorestore',
    ...     '--nsInclude', 'my_source_database.my_collection',
    ...     '--nsFrom', 'my_source_database.*',
    ...     '--nsTo', 'my_destination_database.*',
    ...     '--drop',
    ...     '--stopOnError',
    ...     '--gzip',
    ...     '--dir', '/path/to/dump/folder',
    ...     '--some-other-key', 'some-other-value',
    ... ]
    True
    """

    shell_command_parts = [
        str(mongorestore_path),
    ]

    # If the caller identified specified collections to restore, make the corresponding command parts.
    # Docs: https://www.mongodb.com/docs/database-tools/mongorestore/#std-option-mongorestore.--nsInclude
    if isinstance(collection_names, list):
        for collection_name in collection_names:
            shell_command_parts.extend(["--nsInclude", f"{source_database_name}.{collection_name}"])

    shell_command_parts.extend(
        [
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
    )
    shell_command_parts.extend(destination_server_cli_options)
    return shell_command_parts
