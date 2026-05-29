import logging
import sys
from enum import Enum
from importlib.metadata import version
from logging import getLogger
from pathlib import Path
from typing import Annotated

import pymongo
import typer
from rich import print
from mongo_diff.mongo_diff import Comparator

from src.lib.bookkeeping import Bookkeeper, MigrationEvent
from src.lib.config import (
    RESERVED_GIT_TAGS,
    DatabaseConfig,
    MigrationConfig,
    ParamValidators,
    get_reserved_git_tags_help_snippet,
)
from src.lib.display import (
    make_progress_indicator_for_bounded_task,
    make_progress_indicator_for_unbounded_task,
)
from src.lib.dumps import (
    build_mongodump_command,
    build_mongorestore_command,
)
from src.lib.roles import (
    revoke_standard_role_privileges,
    restore_standard_role_privileges,
)
from src.lib.schema import (
    create_schema_definition,
    create_validator,
    get_migrator_class,
    get_mongo_adapter_class,
    validate_document,
)
from src.lib.system import (
    copy_contents_of_directory,
    delete_contents_of_directory,
    dump_subprocess_failure_and_raise_runtime_error,
    ensure_pip_is_available,
    is_directory_empty,
    run_subprocess_with_live_display,
)

logger = getLogger(name=__name__)

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="rich",
    help="Do things related to migrating the NMDC database.",
    # Note: We hide local variables from the error dumps shown by Rich, so that the passwords
    #       the user might have specified via CLI options or environment variables don't get
    #       printed in those dumps (which could get preserved by observability tools).
    pretty_exceptions_show_locals=False,
)


class AccessManagementOperation(str, Enum):
    """Name of the access management operation to perform on the MongoDB server."""

    REVOKE = "revoke"
    RESTORE = "restore"


class RichHelpPanelName(Enum):
    """Names of Rich Help Panels into which CLI parameters can be organized."""

    SYSTEM = "System"
    MIGRATOR = "Migrator"
    ORIGIN_DATABASE = "Origin MongoDB Database"
    TRANSFORMER_DATABASE = "Transformer MongoDB Database"


@app.command()
def migrate(
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
            help="Hostname for the origin MongoDB server.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ],
    origin_mongo_port: Annotated[
        int,
        typer.Option(
            envvar="ORIGIN_MONGO_PORT",
            help="Port number for the origin MongoDB server.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = 27017,
    origin_mongo_username: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_USERNAME",
            help="Username for the origin MongoDB server. Leave empty for no auth.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = "",
    origin_mongo_password: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_PASSWORD",
            help="Password for the origin MongoDB server.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = "",
    origin_mongo_database_name: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_MONGO_DATABASE_NAME",
            help="Name of the origin MongoDB database.",
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
        ),
    ] = "nmdc",
    # Reference: https://www.mongodb.com/docs/drivers/go/current/connect/connection-targets/#direct-connection
    origin_mongo_direct_connection: Annotated[
        bool,
        typer.Option(
            envvar="ORIGIN_MONGO_DIRECT_CONNECTION",
            help="Whether to use the `directConnection` option when connecting to the origin MongoDB server.",
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
    initial_origin_dump_path: Annotated[
        Path | None,
        typer.Option(
            "--initial-origin-dump",
            envvar="INITIAL_ORIGIN_DUMP",
            help=(
                "Path to a dump (i.e. a directory tree created via `mongodump --gzip ...`) to use "
                "as the initial data (instead of dumping it from the origin MongoDB server). This "
                "can be useful during test-driven development of migrators and is often paired "
                "with the `--skip-origin-writes` option."
            ),
            rich_help_panel=RichHelpPanelName.ORIGIN_DATABASE.value,
            dir_okay=True,
            file_okay=False,
            exists=True,
            resolve_path=True,
        ),
    ] = None,
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
            help="Hostname for the transformer MongoDB server.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = "localhost",
    transformer_mongo_port: Annotated[
        int,
        typer.Option(
            envvar="TRANSFORMER_MONGO_PORT",
            help="Port number for the transformer MongoDB server.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = 27017,
    transformer_mongo_username: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_USERNAME",
            help="Username for the transformer MongoDB server. Leave empty for no auth.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = "",
    transformer_mongo_password: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_PASSWORD",
            help="Password for the transformer MongoDB server.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = "",
    transformer_mongo_database_name: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_MONGO_DATABASE_NAME",
            help="Name of the transformer MongoDB database.",
            rich_help_panel=RichHelpPanelName.TRANSFORMER_DATABASE.value,
        ),
    ] = "transformer",
    # Reference: https://www.mongodb.com/docs/drivers/go/current/connect/connection-targets/#direct-connection
    transformer_mongo_direct_connection: Annotated[
        bool,
        typer.Option(
            envvar="TRANSFORMER_MONGO_DIRECT_CONNECTION",
            help="Whether to use the `directConnection` option when connecting to the transformer MongoDB server.",
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
    show_diff: Annotated[
        bool,
        typer.Option(
            envvar="SHOW_DIFF",
            help="Whether to show a before-and-after-migration diff of the specified collections.",
        ),
    ] = False,
    skip_origin_writes: Annotated[
        bool,
        typer.Option(
            envvar="SKIP_ORIGIN_WRITES",
            help="When set, the app won't make [bold]any[/bold] changes to the origin MongoDB server (e.g. no revoking/restoring user access, no persisting transformed data) and, therefore, does not require write access to it.",
        ),
    ] = False,
    log_file_path: Annotated[
        Path | None,
        typer.Option(
            "--log-file",
            envvar="LOG_FILE",
            help="Path to the file in which you want the app to store its logs.",
            rich_help_panel=RichHelpPanelName.SYSTEM.value,
            dir_okay=False,
            file_okay=True,
            resolve_path=True,
        ),
    ] = None,
) -> None:
    r"""
    Migrate the NMDC database between two versions of the NMDC schema.

    The origin database is the database you want to migrate. This app will dump data from the origin
    database, load it into the transformer database, transform it there so that it conforms to the
    destination schema, validate it there, dump the transformed data from the transformer database,
    and load it into the origin database (overwriting the original data there).

    [bold]Limitation:[/bold] This app does not support migrators that create, rename, and/or delete
    MongoDB collections.
    """

    origin_mongo_database_config = DatabaseConfig(
        host=origin_mongo_host,
        port=origin_mongo_port,
        username=origin_mongo_username,
        password=origin_mongo_password,
        name=origin_mongo_database_name,
        direct_connection=origin_mongo_direct_connection,
    )

    transformer_mongo_database_config = DatabaseConfig(
        host=transformer_mongo_host,
        port=transformer_mongo_port,
        username=transformer_mongo_username,
        password=transformer_mongo_password,
        name=transformer_mongo_database_name,
        direct_connection=transformer_mongo_direct_connection,
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
        initial_origin_dump_path=initial_origin_dump_path,
        transformer_dump_folder_path=transformer_dump_folder_path,
        auto_empty_origin_dump_folder=auto_empty_origin_dump_folder,
        auto_empty_transformer_dump_folder=auto_empty_transformer_dump_folder,
        auto_drop_transformer_database=auto_drop_transformer_database,
        show_diff=show_diff,
        log_file_path=log_file_path,
    )

    # If the user opted to use a log file, attach a file handler to the logger.
    if isinstance(cfg.log_file_path, Path):
        log_level = logging.DEBUG
        handler = logging.FileHandler(cfg.log_file_path, mode="w", encoding="utf-8")
        handler.setLevel(log_level)
        logger.handlers.clear()  # remove any existing handlers
        logger.setLevel(log_level)
        logger.addHandler(handler)
        logger.debug(f"Configuration: {cfg.get_redacted_dict()}")

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

    # If the script is configured to source the initial data from an existing dump, do a few things:
    # (a) warn the user if the didn't also use `--skip-origin-writes`; (b) ensure the existing dump
    # isn't located in the same directory into which we would copy it; and (c) ensure a database
    # having the same name as the origin database exists in the dump.
    #
    # TODO: Consider allowing (b); i.e. allowing both directories to be the same.
    #
    if isinstance(cfg.initial_origin_dump_path, Path):
        if not skip_origin_writes:
            print(
                "[yellow]Warning:[/yellow] An initial origin dump path was provided, but the "
                "`--skip-origin-writes` option was not specified, so the app will still write the"
                "transformed data back to the origin MongoDB server at the end of the migration."
            )
        if cfg.initial_origin_dump_path == cfg.origin_dump_folder_path:
            raise typer.BadParameter(
                "The `--initial-origin-dump` path must differ from the `--origin-dump-folder-path` "
                "path so the app can copy data from the former into the latter."
            )
        if not (cfg.initial_origin_dump_path / cfg.origin_mongo_database_config.name).is_dir():
            raise typer.BadParameter(
                f"Initial origin dump '{cfg.initial_origin_dump_path}' does not contain a "
                f"'{cfg.origin_mongo_database_config.name}' subdirectory."
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
        package_identifier = f"{cfg.schema_repo_url}@{cfg.migrator_git_tag}"
        ensure_pip_is_available(sys.executable)
        command_parts = [sys.executable, "-m", "pip", "install", f"git+{package_identifier}"]
        run_subprocess_with_live_display(
            command_parts,
            task_description=f"Installing {package_identifier}",
            on_error=dump_subprocess_failure_and_raise_runtime_error,
        )
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
        print(f"Origin MongoDB server version: {origin_mongo_server_version}")
        if cfg.origin_mongo_database_config.name not in origin_mongo_client.list_database_names():
            raise typer.BadParameter(f"Origin database '{cfg.origin_mongo_database_config.name}' does not exist.")

    # Connect to the "transformer" MongoDB server and perform a sanity test of the connection.
    transformer_mongo_client = pymongo.MongoClient(**cfg.transformer_mongo_database_config.get_pymongo_client_kwargs())
    with pymongo.timeout(3):
        # Display the MongoDB server version and confirm the "transformer" database does NOT exist yet.
        transformer_mongo_server_version = transformer_mongo_client.server_info()["version"]
        print(f"Transformer MongoDB server version: {transformer_mongo_server_version}")
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

    # If we aren't in "skip origin writes" mode, revoke user access to the "origin" MongoDB server.
    if skip_origin_writes:
        print("[yellow]Skipping revoking privileges on origin MongoDB server.[/yellow]")
    else:
        _ = revoke_standard_role_privileges(admin_database=origin_mongo_client["admin"])
        print("[green]Revoked standard role privileges on origin MongoDB server.[/green]")

    # If the app is configured to get the origin data from an existing dump instead of from the
    # "origin" MongoDB server, copy that dump into the origin dump folder now. Otherwise, dump
    # the subject collections from the "origin" MongoDB server into the origin dump folder now.
    if isinstance(cfg.initial_origin_dump_path, Path):
        print(f"Copying dump from '{cfg.initial_origin_dump_path}' into '{cfg.origin_dump_folder_path}'.")
        copy_contents_of_directory(
            path_to_source_dir=cfg.initial_origin_dump_path,
            path_to_destination_dir=cfg.origin_dump_folder_path,
        )
        print("[green]Copied dump.[/green]")
    else:
        # Dump the subject collections from the "origin" MongoDB server.
        #
        # Note: When making this progress indicator, we disable auto-refresh. That's because we will be
        #       displaying it via a manually-created "live display", which has its own refresh routine.
        #
        progress = make_progress_indicator_for_bounded_task(
            auto_refresh=False,
            show_task_progress_percentage=False,
        )
        task = progress.add_task(
            description="Dumping collections from origin MongoDB database", total=len(cfg.collection_names)
        )
        for collection_name in cfg.collection_names:
            with cfg.origin_mongo_database_config.make_cli_options_context() as database_cli_options:
                shell_command_parts = build_mongodump_command(
                    mongodump_path=cfg.mongodump_path,
                    collection_name=collection_name,
                    dump_folder_path=cfg.origin_dump_folder_path,
                    database_cli_options=database_cli_options,
                )
                run_subprocess_with_live_display(
                    shell_command_parts,
                    task_description=f"Dumping collection '{collection_name}' from origin MongoDB database",
                    progress=progress,
                    on_error=dump_subprocess_failure_and_raise_runtime_error,
                )
            progress.update(task, advance=1)
        print("[green]Dumped collections from origin MongoDB database.[/green]")

    # Restore the subject collections dumped from the "origin" MongoDB server into the "transformer" MongoDB server.
    progress = make_progress_indicator_for_unbounded_task(auto_refresh=False)
    progress.add_task(description="Restoring collections into transformer MongoDB database", total=None)
    with cfg.transformer_mongo_database_config.make_cli_options_context(
        include_db_option=False
    ) as database_cli_options:
        shell_command_parts = build_mongorestore_command(
            mongorestore_path=cfg.mongorestore_path,
            source_database_name=cfg.origin_mongo_database_config.name,
            destination_database_name=cfg.transformer_mongo_database_config.name,
            dump_folder_path=cfg.origin_dump_folder_path,
            destination_server_cli_options=database_cli_options,
        )
        run_subprocess_with_live_display(
            shell_command_parts,
            task_description="Restoring collections into transformer MongoDB database",
            progress=progress,
            on_error=dump_subprocess_failure_and_raise_runtime_error,
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
            # Explicitly refresh the task's progress bar, in case validation took place completely
            # in between refresh intervals (in which case, the bar may remain "empty").
            progress.update(task, refresh=True)
            progress.update(task_outer, advance=1)
    print("[green]Validated documents within transformer MongoDB database.[/green]")

    # Dump the (now-transformed) subject collections from the "transformer" MongoDB server.
    progress = make_progress_indicator_for_bounded_task(
        auto_refresh=False,
        show_task_progress_percentage=False,
    )
    task = progress.add_task(
        description="Dumping collections from transformer MongoDB database", total=len(cfg.collection_names)
    )
    for collection_name in cfg.collection_names:
        with cfg.transformer_mongo_database_config.make_cli_options_context() as database_cli_options:
            shell_command_parts = build_mongodump_command(
                mongodump_path=cfg.mongodump_path,
                collection_name=collection_name,
                dump_folder_path=cfg.transformer_dump_folder_path,
                database_cli_options=database_cli_options,
            )
            run_subprocess_with_live_display(
                shell_command_parts,
                task_description=f"Dumping collection '{collection_name}' from transformer MongoDB database",
                progress=progress,
                on_error=dump_subprocess_failure_and_raise_runtime_error,
            )
        progress.update(task, advance=1)
    print("[green]Dumped collections from transformer MongoDB database.[/green]")

    # If we are not running in "skip origin writes" mode, create a bookkeeper that can be used to
    # record migration events in the "origin" MongoDB server. The bookkeeper's initialization code
    # (a.k.a. constructor) creates a "view" in certain situations, and we don't want it to do that
    # if we are in "skip origin writes" mode.
    bookkeeper = None
    if skip_origin_writes:
        print("[yellow]Skipping creating a bookkeeper for the origin MongoDB server.[/yellow]")
    else:
        bookkeeper = Bookkeeper(
            mongo_client=origin_mongo_client,
            database_name=cfg.origin_mongo_database_config.name,
        )

    # If we aren't in "skip origin writes" mode, record migration events and restore the transformed
    # data into the "origin" MongoDB server.
    if skip_origin_writes:
        print("[yellow]Skipping storing 'MIGRATION_STARTED' event in origin MongoDB database.[/yellow]")
        print("[yellow]Skipping loading transformed data into origin MongoDB database.[/yellow]")
        print("[yellow]Skipping storing 'MIGRATION_COMPLETED' event in origin MongoDB database.[/yellow]")
    else:
        # If we have a bookkeeper, use it to record an event indicating that a migration has started.
        if isinstance(bookkeeper, Bookkeeper):
            bookkeeper.record_migration_event(
                event=MigrationEvent.MIGRATION_STARTED,
                from_schema_version=migrator.get_origin_version(),
                to_schema_version=migrator.get_destination_version(),
                name_of_migrator_module=cfg.migrator_module_name,
            )
            print("[green]Stored 'MIGRATION_STARTED' event in origin MongoDB database.[/green]")

        # Restore the collections dumped from the "transformer" MongoDB server into the "origin" MongoDB
        # server, dropping the original collections from the latter.
        # Docs: https://www.mongodb.com/docs/database-tools/mongorestore/#std-option-mongorestore.--drop
        progress = make_progress_indicator_for_unbounded_task(auto_refresh=False)
        task = progress.add_task(description="Restoring collections into origin MongoDB database", total=None)
        with cfg.origin_mongo_database_config.make_cli_options_context(include_db_option=False) as database_cli_options:
            shell_command_parts = build_mongorestore_command(
                mongorestore_path=cfg.mongorestore_path,
                source_database_name=cfg.transformer_mongo_database_config.name,
                destination_database_name=cfg.origin_mongo_database_config.name,
                dump_folder_path=cfg.transformer_dump_folder_path,
                destination_server_cli_options=database_cli_options,
            )
            run_subprocess_with_live_display(
                shell_command_parts,
                task_description="Restoring collections into origin MongoDB database",
                progress=progress,
                on_error=dump_subprocess_failure_and_raise_runtime_error,
            )
        progress.update(task, advance=1)
        print("[green]Restored collections into origin MongoDB database.[/green]")

        # If we have a bookkeeper, use it to record an event indicating that a migration has been completed.
        if isinstance(bookkeeper, Bookkeeper):
            bookkeeper.record_migration_event(
                event=MigrationEvent.MIGRATION_COMPLETED,
                from_schema_version=migrator.get_origin_version(),
                to_schema_version=migrator.get_destination_version(),
                name_of_migrator_module=cfg.migrator_module_name,
            )
            print("[green]Stored 'MIGRATION_COMPLETED' event in origin MongoDB database.[/green]")

    # Restore user access to the "origin" MongoDB server.
    if skip_origin_writes:
        print("[yellow]Skipping restoring privileges on origin MongoDB server.[/yellow]")
    else:
        _ = restore_standard_role_privileges(admin_database=origin_mongo_client["admin"])
        print("[green]Restored standard role privileges on origin MongoDB server.[/green]")

    # Generate a before-and-after diff of each collection, if the user requested that we do so.
    if cfg.show_diff:
        print("Generating before-and-after diff of each collection.")
        # Restore the subject collections dumped from the "origin" MongoDB server into the "transformer" MongoDB server.
        #
        # Note: In case this seems redundant to you; We, indeed, already restored this dump into the
        #       transformer server. However, we did it _before_ running the migrators, which will
        #       have since transformed that restored data (indeed, that transformed data will act as
        #       our "after" collections here). So, we restore the original ones again, in their
        #       never-transformed state (indeed, they will act as our "before" collections here).
        #       Here, we restore them into a database named `__before` (to avoid overwriting the
        #       existing transformation database).
        #
        progress = make_progress_indicator_for_unbounded_task(auto_refresh=False)
        progress.add_task(description='Restoring "before" collections into transformer MongoDB server', total=None)
        with cfg.transformer_mongo_database_config.make_cli_options_context(
            include_db_option=False
        ) as database_cli_options:
            shell_command_parts = build_mongorestore_command(
                mongorestore_path=cfg.mongorestore_path,
                source_database_name=cfg.origin_mongo_database_config.name,
                destination_database_name="__before",
                dump_folder_path=cfg.origin_dump_folder_path,
                destination_server_cli_options=database_cli_options,
            )
            run_subprocess_with_live_display(
                shell_command_parts,
                task_description='Restoring "before" collections into transformer MongoDB server',
                progress=progress,
                on_error=dump_subprocess_failure_and_raise_runtime_error,
            )
        print('[green]Restored "before" collections into transformer MongoDB server.[/green]')

        comparator = Comparator()
        with make_progress_indicator_for_bounded_task() as progress:
            task_outer = progress.add_task(description="Comparing collections", total=len(cfg.collection_names))
            for collection_name in cfg.collection_names:
                progress.console.print(f"Comparing documents in collection '{collection_name}'")
                diff_result = comparator.compare_collections(
                    collection_a=transformer_mongo_client["__before"].get_collection(collection_name),
                    collection_b=transformer_db.get_collection(collection_name),
                    identifier_field_name_a="id",
                    identifier_field_name_b="id",
                    ignore_oid=False,
                )
                # Print the colorized diff of these two collections, to the console.
                colorized_diff_lines = diff_result.get_all_colorized_diff_lines()
                for line in colorized_diff_lines:
                    print(line)

                    # If the user specified a path to a log file, write the diff lines there, too.
                    if isinstance(cfg.log_file_path, Path):
                        logger.debug(line.rstrip())  # the log handler will already append a newline

                print(diff_result.get_summary_table(title=f"Summary of differences ({collection_name})"))
                progress.update(task_outer, advance=1)
        print("[green]Generated before-and-after diff of each collection.[/green]")

        # Drop the `__before` database.
        transformer_mongo_client.drop_database("__before")
        print('[green]Dropped "before" collections from transformer MongoDB server.[/green]')

    # Done.
    pass


@app.command()
def manage_mongo_access(
    operation: Annotated[
        AccessManagementOperation,
        typer.Option(help="Name of the access management operation you want to perform."),
    ],
    host: Annotated[
        str,
        typer.Option(
            envvar="MONGO_HOST",
            help="Hostname for the MongoDB server.",
        ),
    ] = "localhost",
    port: Annotated[
        int,
        typer.Option(
            envvar="MONGO_PORT",
            help="Port number for the MongoDB server.",
        ),
    ] = 27017,
    username: Annotated[
        str,
        typer.Option(
            envvar="MONGO_USERNAME",
            help="Username for the MongoDB server. Leave empty for no auth.",
        ),
    ] = "",
    password: Annotated[
        str,
        typer.Option(
            envvar="MONGO_PASSWORD",
            help="Password for the MongoDB server.",
        ),
    ] = "",
    direct_connection: Annotated[
        bool,
        typer.Option(
            envvar="MONGO_DIRECT_CONNECTION",
            help="Whether to use the `directConnection` option when connecting to the MongoDB server.",
        ),
    ] = True,
):
    r"""
    Revoke or restore all privileges and role inheritance from the standard NMDC roles
    (except the `nmdc_migrator` role, which this command will not modify).

    The latter can be useful to recover from the scenario in which the `migrate` command is
    interrupted between the time it revokes user access and the time it restores user access.
    """

    database_config = DatabaseConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        name="",
        direct_connection=direct_connection,
    )

    # Connect to the MongoDB server and perform a sanity test of the connection.
    mongo_client = pymongo.MongoClient(**database_config.get_pymongo_client_kwargs())
    with pymongo.timeout(3):
        # Display the MongoDB server version.
        mongo_server_version = mongo_client.server_info()["version"]
        print(f"MongoDB server version: {mongo_server_version}")

    if operation == AccessManagementOperation.RESTORE:
        _ = restore_standard_role_privileges(admin_database=mongo_client["admin"])
        print("[green]Restored standard role privileges on MongoDB server.[/green]")
    elif operation == AccessManagementOperation.REVOKE:
        _ = revoke_standard_role_privileges(admin_database=mongo_client["admin"])
        print("[green]Revoked standard role privileges on MongoDB server.[/green]")
    else:
        raise typer.BadParameter(f"Unsupported operation: {operation}")


if __name__ == "__main__":
    app()
