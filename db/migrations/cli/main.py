from enum import Enum
from pathlib import Path
from importlib.metadata import version
import sys
from typing import Annotated
from logging import getLogger

import pymongo
import typer
from rich import print

from lib.bookkeeping import Bookkeeper, MigrationEvent
from lib.config import (
    RESERVED_GIT_TAGS,
    DatabaseConfig,
    MigrationConfig,
    ParamValidators,
    get_reserved_git_tags_help_snippet,
)
from lib.display import (
    make_progress_indicator_for_bounded_task,
    make_progress_indicator_for_unbounded_task,
)
from lib.roles import (
    revoke_standard_role_privileges,
    restore_standard_role_privileges,
)
from lib.schema import (
    create_schema_definition,
    create_validator,
    get_migrator_class,
    get_mongo_adapter_class,
    validate_document,
)
from lib.system import delete_contents_of_directory, ensure_pip_is_available, is_directory_empty, run_subprocess

logger = getLogger(name=__name__)

app = typer.Typer()


class RichHelpPanelName(Enum):
    """Names of Rich Help Panels into which CLI parameters can be organized."""

    SYSTEM = "System"
    MIGRATOR = "Migrator"
    ORIGIN_DATABASE = "Origin MongoDB Database"
    TRANSFORMER_DATABASE = "Transformer MongoDB Database"


# TODO: Add tests (use mock mongo server? and mock migrator?).
def main(
    migrator_git_tag: Annotated[
        str,
        typer.Option(
            envvar="MIGRATOR_GIT_TAG",
            help=(
                "Git tag of an nmdc-schema commit containing the migrator you want to run. "
                f"Special values: {get_reserved_git_tags_help_snippet()}"
            ),
            rich_help_panel=RichHelpPanelName.MIGRATOR.value,
        ),
    ],
    migrator_module_name: Annotated[
        str,
        typer.Option(
            envvar="MIGRATOR_MODULE_NAME",
            help="Name of the Python module that constitutes the migrator you want to run.",
            rich_help_panel=RichHelpPanelName.MIGRATOR.value,
        ),
    ],
    collection_names: Annotated[
        list[str],
        typer.Option(
            "-c",
            "--collection",
            envvar="COLLECTION_NAMES",
            help="Names of MongoDB collections to migrate. You can specify this option multiple times, or populate the environment variable with a space-delimited list of names.",
            callback=ParamValidators.validate_collection_names,
            rich_help_panel=RichHelpPanelName.MIGRATOR.value,
        ),
    ],
    origin_mongo_host: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_HOST",
            help="Hostname for the origin MongoDB database.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ],
    origin_mongo_port: Annotated[
        int,
        typer.Option(
            envvar="ORIGIN_MONGO_PORT",
            help="Port number for the origin MongoDB database.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = 27017,
    origin_mongo_username: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_USERNAME",
            help="Username for the origin MongoDB database. Leave empty for no auth.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = "",
    origin_mongo_password: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_PASSWORD",
            help="Password for the origin MongoDB database.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = "",
    origin_mongo_database_name: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_DATABASE_NAME",
            help="Database name for the origin MongoDB database.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = "nmdc",
    # Reference: https://www.mongodb.com/docs/drivers/go/current/connect/connection-targets/#direct-connection
    origin_mongo_direction_connection: Annotated[
        bool,
        typer.Option(
            envvar="ORIGIN_MONGO_DIRECT_CONNECTION",
            help="Whether to use the `directConnection` option when connecting to the origin MongoDB database.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = True,
    origin_dump_folder_path: Annotated[
        Path,
        typer.Option(
            envvar="ORIGIN_DUMP_FOLDER_PATH",
            help="Path to the directory in which you want the origin database dump to be created.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("/tmp/mongodump.origin.out"),
    auto_empty_origin_dump_folder: Annotated[
        bool,
        typer.Option(
            envvar="AUTO_EMPTY_ORIGIN_DUMP_FOLDER",
            help="Whether to automatically delete the contents of the origin dump folder before use, if it is not empty. By default, the script will abort in that situation.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = False,
    transformer_mongo_host: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_HOST",
            help="Hostname for the transformer MongoDB database.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = "localhost",
    transformer_mongo_port: Annotated[
        int,
        typer.Option(
            envvar="TRANSFORMER_MONGO_PORT",
            help="Port number for the transformer MongoDB database.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = 27017,
    transformer_mongo_username: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_USERNAME",
            help="Username for the transformer MongoDB database. Leave empty for no auth.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = "",
    transformer_mongo_password: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_PASSWORD",
            help="Password for the transformer MongoDB database.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = "",
    transformer_mongo_database_name: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_DATABASE_NAME",
            help="Database name for the transformer MongoDB database.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = "transformer",
    # Reference: https://www.mongodb.com/docs/drivers/go/current/connect/connection-targets/#direct-connection
    transformer_mongo_direction_connection: Annotated[
        bool,
        typer.Option(
            envvar="TRANSFORMER_MONGO_DIRECT_CONNECTION",
            help="Whether to use the `directConnection` option when connecting to the transformer MongoDB database.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = True,
    transformer_dump_folder_path: Annotated[
        Path,
        typer.Option(
            envvar="TRANSFORMER_DUMP_FOLDER_PATH",
            help="Path to the directory in which you want the transformer database dump to be created.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("/tmp/mongodump.transformer.out"),
    auto_empty_transformer_dump_folder: Annotated[
        bool,
        typer.Option(
            envvar="AUTO_EMPTY_TRANSFORMER_DUMP_FOLDER",
            help="Whether to automatically delete the contents of the transformer dump folder before use, if it is not empty. By default, the script will abort in that situation.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = False,
    auto_drop_transformer_database: Annotated[
        bool,
        typer.Option(
            envvar="AUTO_DROP_TRANSFORMER_DATABASE",
            help="Whether to automatically drop the transformer database if it already exists. By default, the script will abort in that situation.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = False,
    mongodump_path: Annotated[
        Path,
        typer.Option(
            dir_okay=False,
            exists=True,
            resolve_path=True,
            envvar="MONGODUMP_PATH",
            help="Path to the `mongodump` executable.",
            rich_help_panel=RichHelpPanelName.SYSTEM.value,
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
            rich_help_panel=RichHelpPanelName.SYSTEM.value,
            callback=ParamValidators.validate_executable_file,
        ),
    ] = Path("/usr/bin/mongorestore"),
    schema_repo_url: Annotated[
        str,
        typer.Option(
            envvar="SCHEMA_REPO_URL",
            help="URL of the Git repository containing the NMDC schema and its migrators.",
            rich_help_panel=RichHelpPanelName.MIGRATOR.value,
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
        direct_connection=origin_mongo_direction_connection,
    )

    transformer_mongo_database_config = DatabaseConfig(
        host=transformer_mongo_host,
        port=transformer_mongo_port,
        username=transformer_mongo_username,
        password=transformer_mongo_password,
        name=transformer_mongo_database_name,
        direct_connection=transformer_mongo_direction_connection,
    )

    cfg = MigrationConfig(
        mongodump_path=mongodump_path,
        mongorestore_path=mongorestore_path,
        origin_mongo_database_config=origin_mongo_database_config,
        transformer_mongo_database_config=transformer_mongo_database_config,
        migrator_git_tag=migrator_git_tag,
        migrator_module_name=migrator_module_name,
        schema_repo_url=schema_repo_url,
        collection_names=collection_names,
        origin_dump_folder_path=origin_dump_folder_path,
        transformer_dump_folder_path=transformer_dump_folder_path,
        auto_empty_origin_dump_folder=auto_empty_origin_dump_folder,
        auto_empty_transformer_dump_folder=auto_empty_transformer_dump_folder,
        auto_drop_transformer_database=auto_drop_transformer_database,
    )

    # If the script is configured to access both the origin MongoDB server and the transformer MongoDB server
    # at the same hostname and port, display a warning (since that might not have been intentional).
    if (
        cfg.origin_mongo_database_config.host == cfg.transformer_mongo_database_config.host
        and cfg.origin_mongo_database_config.port == cfg.transformer_mongo_database_config.port
    ):
        print(
            "[yellow]Warning:[/yellow] Accessing origin and transformer MongoDB server at "
            "same hostname and port "
            f"(i.e. '{cfg.origin_mongo_database_config.host}:{cfg.origin_mongo_database_config.port}')."
        )

    # If the origin dump folder is non-empty, abort unless the user has opted to auto-empty it.
    if cfg.origin_dump_folder_path.is_dir() and not is_directory_empty(cfg.origin_dump_folder_path):
        print(f"[yellow]Warning:[/yellow] Origin dump folder '{cfg.origin_dump_folder_path}' is not empty.")
        if cfg.auto_empty_origin_dump_folder:
            print("Emptying origin dump folder automatically.")
            _ = delete_contents_of_directory(cfg.origin_dump_folder_path)
        else:
            raise typer.BadParameter(
                f"Origin dump folder '{cfg.origin_dump_folder_path}' is not empty. "
                "Either empty it manually or use the `--auto-empty-origin-dump-folder` option "
                "to empty it automatically."
            )

    # If the transformer dump folder is non-empty, abort unless the user has opted to auto-empty it.
    if cfg.transformer_dump_folder_path.is_dir() and not is_directory_empty(cfg.transformer_dump_folder_path):
        print(f"[yellow]Warning:[/yellow] Transformer dump folder '{cfg.transformer_dump_folder_path}' is not empty.")
        if cfg.auto_empty_transformer_dump_folder:
            print("Emptying transformer dump folder automatically.")
            _ = delete_contents_of_directory(cfg.transformer_dump_folder_path)
        else:
            raise typer.BadParameter(
                f"Transformer dump folder '{cfg.transformer_dump_folder_path}' is not empty. "
                "Either empty it manually or use the `--auto-empty-transformer-dump-folder` option "
                "to empty it automatically."
            )

    # Use pip to install the `nmdc-schema` version specified by the user.
    if cfg.migrator_git_tag in RESERVED_GIT_TAGS.keys():
        if cfg.migrator_git_tag == "-INSTALLED":
            print(f"Using the currently-installed nmdc-schema package: {version('nmdc_schema')}")
        else:
            # If execution gets here, it means a developer introduced a new reserved Git tag into
            # the `RESERVED_GIT_TAGS` dictionary, but did not update these conditions accordingly.
            raise typer.BadParameter(f"Unsupported reserved Git tag: {cfg.migrator_git_tag}")
    else:
        with make_progress_indicator_for_unbounded_task() as progress:
            package_identifier = f"{cfg.schema_repo_url}@{cfg.migrator_git_tag}"
            progress.add_task(description=f"Installing {package_identifier}", total=None)
            ensure_pip_is_available(sys.executable)
            command_parts = [sys.executable, "-m", "pip", "install", f"git+{package_identifier}"]
            completed_process = run_subprocess(command_parts)
            if completed_process.returncode != 0:
                raise typer.BadParameter(f"Failed to install {package_identifier}. \n\n{completed_process.stderr}")
            else:
                print(f"Installed {package_identifier} using interpreter {sys.executable}")
        print(f"[green]Installed {package_identifier}.[/green]")

    # Dynamically import the migrator module specified by the user and get the `Migrator` class from it.
    Migrator = get_migrator_class(migrator_module_name=cfg.migrator_module_name)

    # Import other classes from it.
    MongoAdapter = get_mongo_adapter_class()

    # Connect to the "origin" MongoDB server and perform a sanity test of the connection.
    origin_mongo_client = pymongo.MongoClient(**cfg.origin_mongo_database_config.get_pymongo_client_kwargs())
    with pymongo.timeout(3):
        # Display the MongoDB server version and confirm the "origin" database DOES exist.
        origin_mongo_server_version = origin_mongo_client.server_info()["version"]
        print(f"Origin Mongo server version: {origin_mongo_server_version}")
        if cfg.origin_mongo_database_config.name not in origin_mongo_client.list_database_names():
            raise typer.BadParameter(f"Origin database '{cfg.origin_mongo_database_config.name}' does not exist.")

    # Connect to the "transformer" MongoDB server and perform a sanity test of the connection.
    transformer_mongo_client = pymongo.MongoClient(**cfg.transformer_mongo_database_config.get_pymongo_client_kwargs())
    with pymongo.timeout(3):
        # Display the MongoDB server version and confirm the "transformer" database does NOT exist yet.
        transformer_mongo_server_version = transformer_mongo_client.server_info()["version"]
        print(f"Transformer Mongo server version: {transformer_mongo_server_version}")
        if cfg.transformer_mongo_database_config.name in transformer_mongo_client.list_database_names():
            if not cfg.auto_drop_transformer_database:
                raise typer.BadParameter(
                    f"Transformer database '{cfg.transformer_mongo_database_config.name}' already exists. "
                    "Either drop it manually or use the `--auto-drop-transformer-database` option "
                    "to drop it automatically."
                )
            else:
                print(
                    "[yellow]Dropping existing transformer database: "
                    f"{cfg.transformer_mongo_database_config.name}[/yellow]."
                )
                transformer_mongo_client.drop_database(cfg.transformer_mongo_database_config.name)

    # Revoke user access to the "origin" MongoDB server.
    _ = revoke_standard_role_privileges(admin_database=origin_mongo_client["admin"])
    print("[green]Revoked standard role privileges on origin server.[/green]")

    # Dump the subject collections from the "origin" MongoDB server.
    with make_progress_indicator_for_bounded_task() as progress:
        task_outer = progress.add_task(
            description="Dumping collections from origin MongoDB database", total=len(cfg.collection_names)
        )
        for collection_name in cfg.collection_names:
            shell_command_parts = [
                cfg.mongodump_path,
                "--collection",
                collection_name,
                "--gzip",
                "--out",
                cfg.origin_dump_folder_path,
            ]
            shell_command_parts.extend(cfg.origin_mongo_database_config.get_cli_options())
            completed_process = run_subprocess(shell_command_parts)
            if completed_process.returncode != 0:
                raise RuntimeError(
                    f"Failed to dump collection '{collection_name}' from origin MongoDB database."
                    f"\n\n{completed_process.stderr}"
                )
            progress.update(task_outer, advance=1)
        print("[green]Dumped collections from origin MongoDB database.[/green]")

    # Restore the subject collections dumped from the "origin" MongoDB server into the "transformer" MongoDB server.
    with make_progress_indicator_for_unbounded_task() as progress:
        progress.add_task(description="Restoring collections into transformer MongoDB database", total=None)
        shell_command_parts = [
            cfg.mongorestore_path,
            "--nsFrom",
            f"{cfg.origin_mongo_database_config.name}.*",
            "--nsTo",
            f"{cfg.transformer_mongo_database_config.name}.*",
            "--drop",
            "--stopOnError",
            "--gzip",
            "--dir",
            cfg.origin_dump_folder_path,
        ]
        shell_command_parts.extend(cfg.transformer_mongo_database_config.get_cli_options(include_db_option=False))
        completed_process = run_subprocess(shell_command_parts)
        if completed_process.returncode != 0:
            raise RuntimeError(
                f"Failed to restore dump from origin into transformer MongoDB database.\n\n{completed_process.stderr}"
            )
    print("[green]Restored collections into transformer MongoDB database.[/green]")

    # Use the migrator to transform the data within the "transformer" MongoDB server.
    with make_progress_indicator_for_unbounded_task() as progress:
        progress.add_task(description="Migrating data within transformer MongoDB database", total=None)
        transformer_db = transformer_mongo_client[cfg.transformer_mongo_database_config.name]
        adapter = MongoAdapter(database=transformer_db)
        migrator = Migrator(adapter=adapter, logger=logger)
        migrator.upgrade(commit_changes=True)
    print("[green]Migrated data within transformer MongoDB database.[/green]")

    # Validate the transformed data.
    with make_progress_indicator_for_bounded_task() as progress:
        task_outer = progress.add_task(description="Validating collections", total=len(cfg.collection_names))
        schema_definition = create_schema_definition()
        validator = create_validator(schema_definition=schema_definition)
        for collection_name in cfg.collection_names:
            collection = transformer_db.get_collection(collection_name)
            num_documents = collection.count_documents({})
            task = progress.add_task(f"Validating documents in '{collection_name}'", total=num_documents)
            for document in collection.find({}, {"_id": 0}):
                validate_document(document=document, validator=validator)
                progress.update(task, advance=1)
            progress.update(task_outer, advance=1)
    print("[green]Validated documents within transformer MongoDB database.[/green]")

    # Dump the (now-transformed) subject collections from the "transformer" MongoDB server.
    with make_progress_indicator_for_bounded_task() as progress:
        task_outer = progress.add_task(
            description="Dumping collections from transformer MongoDB database", total=len(cfg.collection_names)
        )
        for collection_name in cfg.collection_names:
            shell_command_parts = [
                cfg.mongodump_path,
                "--collection",
                collection_name,
                "--gzip",
                "--out",
                cfg.transformer_dump_folder_path,
            ]
            shell_command_parts.extend(cfg.transformer_mongo_database_config.get_cli_options())
            completed_process = run_subprocess(shell_command_parts)
            if completed_process.returncode != 0:
                raise RuntimeError(
                    f"Failed to dump collection '{collection_name}' from transformer MongoDB database."
                    f"\n\n{completed_process.stderr}"
                )
            progress.update(task_outer, advance=1)
    print("[green]Dumped collections from transformer MongoDB database.[/green]")

    # Create a bookkeeper that can be used to record migration events in the "origin" MongoDB server.
    bookkeeper = Bookkeeper(mongo_client=origin_mongo_client)

    # Record an event that indicates that a migration has started.
    bookkeeper.record_migration_event(
        event=MigrationEvent.MIGRATION_STARTED,
        from_schema_version=migrator.get_origin_version(),
        to_schema_version=migrator.get_destination_version(),
        name_of_migrator_module=cfg.migrator_module_name,
    )
    print("[green]Stored 'MIGRATION_STARTED' event in origin MongoDB database.[/green]")

    # Restore the subject collections dumped from the "transformer" MongoDB server into the "origin" MongoDB server,
    # dropping the original collections.
    # Docs: https://www.mongodb.com/docs/database-tools/mongorestore/#std-option-mongorestore.--drop
    with make_progress_indicator_for_unbounded_task() as progress:
        task = progress.add_task(description="Restoring collections into origin MongoDB database", total=None)
        shell_command_parts = [
            cfg.mongorestore_path,
            "--nsFrom",
            f"{cfg.transformer_mongo_database_config.name}.*",
            "--nsTo",
            f"{cfg.origin_mongo_database_config.name}.*",
            "--drop",
            "--stopOnError",
            "--gzip",
            "--dir",
            cfg.transformer_dump_folder_path,
        ]
        shell_command_parts.extend(cfg.origin_mongo_database_config.get_cli_options(include_db_option=False))
        completed_process = run_subprocess(shell_command_parts)
        if completed_process.returncode != 0:
            raise RuntimeError(
                f"Failed to restore dump from transformer into origin MongoDB database.\n\n{completed_process.stderr}"
            )
        progress.update(task, advance=1)
    print("[green]Restored collections into origin MongoDB database.[/green]")

    # Record an event that indicates that a migration has been completed.
    bookkeeper.record_migration_event(
        event=MigrationEvent.MIGRATION_COMPLETED,
        from_schema_version=migrator.get_origin_version(),
        to_schema_version=migrator.get_destination_version(),
        name_of_migrator_module=cfg.migrator_module_name,
    )
    print("[green]Stored 'MIGRATION_COMPLETED' event in origin MongoDB database.[/green]")

    # Restore user access to the "origin" MongoDB server.
    _ = restore_standard_role_privileges(admin_database=origin_mongo_client["admin"])
    print("[green]Restored standard role privileges on origin server.[/green]")


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
#   $ curl -LsSf https://astral.sh/uv/install.sh | sh
#   $ source $HOME/.local/bin/env
#
