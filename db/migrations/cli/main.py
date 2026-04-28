from inspect import isclass
from os import access, X_OK
from dataclasses import asdict, dataclass
from pathlib import Path
from importlib import import_module
from importlib.metadata import version
import subprocess
import sys
from typing import Annotated, Optional

import pymongo
import typer
from rich import print
from rich.progress import Progress, SpinnerColumn, TextColumn

from lib.roles import (
    revoke_standard_role_privileges,
    restore_standard_role_privileges,
)

app = typer.Typer()


def run_subprocess(command_parts: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a subprocess and capture its text output."""

    return subprocess.run(command_parts, capture_output=True, text=True)


def ensure_pip_is_available(python_executable: str) -> None:
    """Install `pip` into the specified Python environment if it is missing."""

    probe_result = run_subprocess([python_executable, "-m", "pip", "--version"])
    if probe_result.returncode == 0:
        return

    if "No module named pip" not in probe_result.stderr:
        raise typer.BadParameter(f"Failed to probe pip availability.\n\n{probe_result.stderr}")

    bootstrap_result = run_subprocess([python_executable, "-m", "ensurepip", "--upgrade"])
    if bootstrap_result.returncode != 0:
        raise typer.BadParameter(f"Failed to bootstrap pip.\n\n{bootstrap_result.stderr}")


class ParamValidators:
    """Collection of static methods for validating CLI parameters."""

    @staticmethod
    def validate_executable_file(ctx: typer.Context, path: Path) -> Optional[Path]:
        """Check whether the specified path points to an executable file."""

        if ctx.resilient_parsing:  # accommodate CLI autocompletion
            return None
        if not path.is_file() or not access(path, X_OK):
            raise typer.BadParameter(f"{path} is not an executable file")
        return path


# Note: We use `frozen=True` to prevent editing after initial instantiation.
@dataclass(frozen=True)
class DatabaseConfig:
    """Configuration for connecting to a MongoDB database."""

    host: str
    port: int
    username: str
    password: str
    name: str

    @property
    def is_auth_enabled(self) -> bool:
        """Returns True if the database config includes credentials for authentication."""
        return self.username != ""

    def get_redacted_dict(self) -> dict:
        """
        Get a representation of the database config in which sensitive values have been redacted.
        The representation also includes the derived `is_auth_enabled` field, which `asdict()`
        excludes by default.
        """
        config_dict = asdict(self)
        config_dict["password"] = "" if self.password == "" else "***"
        config_dict["is_auth_enabled"] = self.is_auth_enabled
        return config_dict

    def get_pymongo_client_kwargs(self) -> dict:
        """Get a dictionary of keyword arguments for instantiating a `pymongo.MongoClient` with this config."""
        kwargs = {
            "host": self.host,
            "port": self.port,
        }
        if self.is_auth_enabled:
            kwargs.update({"username": self.username, "password": self.password})
        return kwargs

    def get_cli_options(self) -> list[str]:
        """Get a list of CLI options for connecting to a MongoDB database with this config."""
        options = [
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--db",
            self.name,
        ]
        if self.is_auth_enabled:
            options.extend(["--username", self.username, "--password", self.password])
        return options


@dataclass(frozen=True)
class MigrationConfig:
    """Configuration for migrating a MongoDB database from one NMDC schema version to another."""

    mongosh_path: Path
    mongodump_path: Path
    mongorestore_path: Path
    origin_mongo_database_config: DatabaseConfig
    transformer_mongo_database_config: DatabaseConfig
    migrator_git_tag: str
    migrator_module_name: str

    def get_redacted_dict(self) -> dict:
        """Get a representation of the config in which sensitive values have been redacted."""
        config_dict = asdict(self)
        config_dict["origin_mongo_db_config"] = self.origin_mongo_database_config.get_redacted_dict()
        config_dict["transformer_mongo_db_config"] = self.transformer_mongo_database_config.get_redacted_dict()
        return config_dict


# Note: These are basically "sentinel" values that users can specify for the `migrator_git_tag`
#       parameter to get special behavior. Ensure each one begins with a hyphen, since I don't
#       think Git, itself, would allow a tag to begin with a hyphen.
RESERVED_GIT_TAGS: dict[str, str] = {
    "-INSTALLED": (
        "Use the nmdc-schema package already installed in the Python environment "
        "(useful for rapid development and avoiding rate limiting by the Git repository host)."
    ),
}


def get_reserved_git_tags_help_snippet() -> str:
    """Get a help snippet describing the reserved Git tags."""
    return ", ".join(f"'{tag}': {description}" for tag, description in RESERVED_GIT_TAGS.items())


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
    )

    print(config.get_redacted_dict())

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

    # Dynamically import the migrator module specified by the user and get the Migrator class from it.
    print(f"Importing Migrator class from module: {migrator_module_name}")
    migrator_module = import_module(f".{migrator_module_name}", package="nmdc_schema.migrators")
    Migrator = getattr(migrator_module, "Migrator")  # gets the class
    if not isclass(Migrator):
        raise typer.BadParameter(f"Failed to import Migrator from module {migrator_module_name}")

    # If the script is configured to access both the origin MongoDB server and the transformer MongoDB server
    # at the same hostname and port, display a warning (since that might not have been intentional).
    if origin_mongo_host == transformer_mongo_host and origin_mongo_port == transformer_mongo_port:
        print("[yellow]Warning: Accessing origin and transformer MongoDB server at same hostname and port.[/yellow]")

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
