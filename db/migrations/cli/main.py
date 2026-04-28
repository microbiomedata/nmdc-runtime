from inspect import isclass
from pathlib import Path
from importlib import import_module
from importlib.metadata import version
import sys
from typing import Annotated

import pymongo
import typer
from rich import print
from rich.progress import Progress, SpinnerColumn, TextColumn

from lib.config import (
    RESERVED_GIT_TAGS,
    DatabaseConfig,
    MigrationConfig,
    ParamValidators,
    get_reserved_git_tags_help_snippet,
)
from lib.roles import (
    revoke_standard_role_privileges,
    restore_standard_role_privileges,
)
from lib.system import ensure_pip_is_available, run_subprocess

app = typer.Typer()


# TODO: Add a parameter that controls the `directConnection` flag.
#       See: https://www.mongodb.com/docs/drivers/go/current/connect/connection-targets/#direct-connection
def main(
    migrator_git_tag: Annotated[
        str,
        typer.Option(
            envvar="MIGRATOR_GIT_TAG",
            help=(
                "Git tag of an nmdc-schema commit containing the migrator you want to run. "
                f"Special values: {get_reserved_git_tags_help_snippet()}"
            ),
        ),
    ],
    migrator_module_name: Annotated[
        str,
        typer.Option(
            envvar="MIGRATOR_MODULE_NAME",
            help="Name of the Python module that constitutes the migrator you want to run.",
        ),
    ],
    origin_mongo_host: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_HOST",
            help="Hostname for the origin MongoDB database.",
        ),
    ],
    origin_mongo_port: Annotated[
        int,
        typer.Option(
            envvar="ORIGIN_MONGO_PORT",
            help="Port number for the origin MongoDB database.",
        ),
    ] = 27017,
    origin_mongo_username: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_USERNAME",
            help="Username for the origin MongoDB database. Leave empty for no auth.",
        ),
    ] = "",
    origin_mongo_password: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_PASSWORD",
            help="Password for the origin MongoDB database.",
        ),
    ] = "",
    origin_mongo_database_name: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_DATABASE_NAME",
            help="Database name for the origin MongoDB database.",
        ),
    ] = "nmdc",
    origin_dump_folder_path: Annotated[
        Path,
        typer.Option(
            envvar="ORIGIN_DUMP_FOLDER_PATH",
            help="Path to the directory in which you want the origin database dump to be created.",
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("/tmp/mongodump.origin.out"),
    transformer_mongo_host: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_HOST",
            help="Hostname for the transformer MongoDB database.",
        ),
    ] = "localhost",
    transformer_mongo_port: Annotated[
        int,
        typer.Option(
            envvar="TRANSFORMER_MONGO_PORT",
            help="Port number for the transformer MongoDB database.",
        ),
    ] = 27017,
    transformer_mongo_username: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_USERNAME",
            help="Username for the transformer MongoDB database. Leave empty for no auth.",
        ),
    ] = "",
    transformer_mongo_password: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_PASSWORD",
            help="Password for the transformer MongoDB database.",
        ),
    ] = "",
    transformer_mongo_database_name: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_DATABASE_NAME",
            help="Database name for the transformer MongoDB database.",
        ),
    ] = "transformer",
    transformer_dump_folder_path: Annotated[
        Path,
        typer.Option(
            envvar="TRANSFORMER_DUMP_FOLDER_PATH",
            help="Path to the directory in which you want the transformer database dump to be created.",
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("/tmp/mongodump.transformer.out"),
    mongosh_path: Annotated[
        Path,
        typer.Option(
            dir_okay=False,
            exists=True,
            resolve_path=True,
            envvar="MONGOSH_PATH",
            help="Path to the `mongosh` executable.",
            callback=ParamValidators.validate_executable_file,
        ),
    ] = Path("/usr/bin/mongosh"),
    mongodump_path: Annotated[
        Path,
        typer.Option(
            dir_okay=False,
            exists=True,
            resolve_path=True,
            envvar="MONGODUMP_PATH",
            help="Path to the `mongodump` executable.",
            callback=ParamValidators.validate_executable_file,
        ),
    ] = Path("/usr/bin/mongodump"),
    mongorestore_path: Annotated[
        Path,
        typer.Option(
            dir_okay=False,
            exists=True,
            resolve_path=True,
            envvar="MONGORESTORE_PATH",
            help="Path to the `mongorestore` executable.",
            callback=ParamValidators.validate_executable_file,
        ),
    ] = Path("/usr/bin/mongorestore"),
    schema_repo_url: Annotated[
        str,
        typer.Option(
            envvar="SCHEMA_REPO_URL",
            help="URL of the Git repository containing the NMDC schema and its migrators.",
        ),
    ] = "https://github.com/microbiomedata/nmdc-schema.git",
) -> None:
    """
    Migrate the NMDC database between two versions of the NMDC schema.

    The origin database is the database you want to migrate. This app will dump data from the origin
    database, load it into the transformer database, transform it there so that it conforms to the
    destination schema, validate it there, dump the transformed data from the transformer database,
    and load it into the origin database (overwriting the original data there).

    This app does not support migrators that involve renaming MongoDB collection.
    """

    origin_mongo_database_config = DatabaseConfig(
        host=origin_mongo_host,
        port=origin_mongo_port,
        username=origin_mongo_username,
        password=origin_mongo_password,
        name=origin_mongo_database_name,
    )

    transformer_mongo_database_config = DatabaseConfig(
        host=transformer_mongo_host,
        port=transformer_mongo_port,
        username=transformer_mongo_username,
        password=transformer_mongo_password,
        name=transformer_mongo_database_name,
    )

    config = MigrationConfig(
        mongosh_path=mongosh_path,
        mongodump_path=mongodump_path,
        mongorestore_path=mongorestore_path,
        origin_mongo_database_config=origin_mongo_database_config,
        transformer_mongo_database_config=transformer_mongo_database_config,
        migrator_git_tag=migrator_git_tag,
        migrator_module_name=migrator_module_name,
        schema_repo_url=schema_repo_url,
        origin_dump_folder_path=origin_dump_folder_path,
        transformer_dump_folder_path=transformer_dump_folder_path,
    )

    print(config.get_redacted_dict())

    # If the script is configured to access both the origin MongoDB server and the transformer MongoDB server
    # at the same hostname and port, display a warning (since that might not have been intentional).
    if origin_mongo_host == transformer_mongo_host and origin_mongo_port == transformer_mongo_port:
        print("[yellow]Warning: Accessing origin and transformer MongoDB server at same hostname and port.[/yellow]")

    # Use pip to install the `nmdc-schema` version specified by the user.
    if migrator_git_tag in RESERVED_GIT_TAGS.keys():
        if migrator_git_tag == "-INSTALLED":
            print(f"Using the currently-installed nmdc-schema package: {version('nmdc_schema')}")
        else:
            # If execution gets here, it means a developer introduced a new reserved Git tag into
            # the `RESERVED_GIT_TAGS` dictionary, but did not update these conditions accordingly.
            raise typer.BadParameter(f"Unsupported reserved Git tag: {migrator_git_tag}")
    else:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            package_identifier = f"{schema_repo_url}@{migrator_git_tag}"
            progress.add_task(description=f"Installing {package_identifier}", total=None)
            ensure_pip_is_available(sys.executable)
            command_parts = [sys.executable, "-m", "pip", "install", f"git+{package_identifier}"]
            result = run_subprocess(command_parts)
            if result.returncode != 0:
                raise typer.BadParameter(f"Failed to install {package_identifier}.\n\n{result.stderr}")
            else:
                print(f"Installed {package_identifier} using interpreter {sys.executable}")

    # Dynamically import the migrator module specified by the user and get the `Migrator` class from it.
    print(f"Importing Migrator class from module: {migrator_module_name}")
    migrator_module = import_module(f".{migrator_module_name}", package="nmdc_schema.migrators")
    Migrator = getattr(migrator_module, "Migrator")  # gets the class
    if not isclass(Migrator):
        raise typer.BadParameter(f"Failed to import Migrator class from module {migrator_module_name}")
    
    # Also import other modules from the same package.
    mongo_adapter_module = import_module(".mongo_adapter", package="nmdc_schema.migrators.adapters")
    MongoAdapter = getattr(mongo_adapter_module, "MongoAdapter")
    if not isclass(MongoAdapter):
        raise typer.BadParameter(f"Failed to import MongoAdapter class from module {mongo_adapter_module.__name__}")

    # Connect to the origin MongoDB server.
    origin_mongo_client = pymongo.MongoClient(**origin_mongo_database_config.get_pymongo_client_kwargs())

    # Connect to the "transformer" MongoDB server.
    transformer_mongo_client = pymongo.MongoClient(**transformer_mongo_database_config.get_pymongo_client_kwargs())

    # Perform sanity tests.
    with pymongo.timeout(3):
        # Display the MongoDB server version (running on the "origin" server) and confirm the "origin" database DOES exist.
        origin_mongo_server_version = origin_mongo_client.server_info()["version"]
        print(f"Origin Mongo server version: {origin_mongo_server_version}")
        if origin_mongo_database_name not in origin_mongo_client.list_database_names():
            raise typer.BadParameter(f"Origin database '{origin_mongo_database_name}' does not exist.")

        # Display the MongoDB server version (running on the "transformer" server) and confirm the "transformer" database does NOT exist yet.
        transformer_mongo_server_version = transformer_mongo_client.server_info()["version"]
        print(f"Transformer Mongo server version: {transformer_mongo_server_version}")
        if transformer_mongo_database_name in transformer_mongo_client.list_database_names():
            raise typer.BadParameter(f"Transformer database '{transformer_mongo_database_name}' already exists.")

    # Revoke user access to the "origin" MongoDB server.
    revoked_roles_result = revoke_standard_role_privileges(admin_database=origin_mongo_client["admin"])
    print(f"Revoked standard role privileges on origin server:\n{revoked_roles_result}")

    # Dump the subject collections from the "origin" MongoDB server.
    # TODO: Get this list of collection names dynamically; either from the environment (e.g. CLI options) or from the `Migrator` class.
    collection_names = ["colors", "shapes"]
    for collection_name in collection_names:
        shell_command_parts = [
            mongodump_path,
            "--collection",
            collection_name,
            "--gzip",
            "--out",
            origin_dump_folder_path,
        ]
        shell_command_parts.extend(origin_mongo_database_config.get_cli_options())
        print(run_subprocess(shell_command_parts))

    # Restore the subject collections dumped from the "origin" MongoDB server into the "transformer" MongoDB server.
    shell_command_parts = [
        mongorestore_path,
        "--nsFrom",
        f"{origin_mongo_database_name}.*",
        "--nsTo",
        f"{transformer_mongo_database_name}.*",
        "--drop",
        "--stopOnError",
        "--gzip",
        "--dir",
        origin_dump_folder_path,
    ]
    shell_command_parts.extend(transformer_mongo_database_config.get_cli_options(include_db_option=False))
    print(run_subprocess(shell_command_parts))

    # Use the migrator to transform the data within the "transformer" MongoDB server.
    # TODO: Configure a `logger` for the migrator to use.
    adapter = MongoAdapter(database=transformer_mongo_client[transformer_mongo_database_name])
    migrator = Migrator(adapter=adapter)
    migrator.upgrade(commit_changes=True)

    # Restore user access to the "origin" MongoDB server.
    restored_roles_result = restore_standard_role_privileges(admin_database=origin_mongo_client["admin"])
    print(f"Restored standard role privileges on origin server:\n{restored_roles_result}")


def run() -> None:
    """Runs the CLI app."""
    typer.run(main)


if __name__ == "__main__":
    run()


# Note:
#
# To install git and curl within the `mongo` container:
#   $ apt update && apt install -y git curl && apt clean
#
# To install uv within the `mongo` container:
#   $ curl -LsSf https://astral.sh/uv/install.sh | s
#   $ source $HOME/.local/bin/env
#
