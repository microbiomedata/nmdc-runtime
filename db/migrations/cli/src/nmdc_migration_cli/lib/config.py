from contextlib import contextmanager
import json
from typing import Optional
from dataclasses import asdict, dataclass
import os
from os import access, X_OK
from pathlib import Path
import tempfile
from typing import Iterator

import typer

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

    @staticmethod
    def validate_collection_names(ctx: typer.Context, collection_names: str | tuple[str, ...]) -> list[str]:
        """
        Confirm all collection names are unique and no collection name is an empty string.
        Then, normalize the collection name(s) into a list of strings.

        Although we _could_ _silently_ de-duplicate the list of collection names, we are opting to
        inform the user about duplicates because it might be indicative of a misunderstanding or a
        typo on their part, and we want them to become aware of it as early as possible.

        >>> from types import SimpleNamespace, new_class
        >>> mock_ctx = SimpleNamespace(resilient_parsing=False)
        >>> ParamValidators.validate_collection_names(mock_ctx, "a")  # single-name string
        ['a']
        >>> ParamValidators.validate_collection_names(mock_ctx, ["a"])  # single-name list
        ['a']
        >>> ParamValidators.validate_collection_names(mock_ctx, "a b")  # multi-name string
        ['a', 'b']
        >>> ParamValidators.validate_collection_names(mock_ctx, ["a", "b"])  # multi-name list
        ['a', 'b']
        >>> ParamValidators.validate_collection_names(mock_ctx, ["a", "a"])  # duplicate names
        Traceback (most recent call last):
        ...
        click.exceptions.BadParameter: Collection name 'a' is specified more than once.
        >>> ParamValidators.validate_collection_names(mock_ctx, ["", "a"])  # empty string in list
        Traceback (most recent call last):
        ...
        click.exceptions.BadParameter: Collection name cannot be an empty string.
        >>> ParamValidators.validate_collection_names(mock_ctx, "")  # empty string
        Traceback (most recent call last):
        ...
        click.exceptions.BadParameter: At least one collection name must be specified.
        >>> ParamValidators.validate_collection_names(mock_ctx, [" "])  # whitespace string in list
        Traceback (most recent call last):
        ...
        click.exceptions.BadParameter: Collection name cannot consist of only whitespace.
        """

        if ctx.resilient_parsing:  # accommodate CLI autocompletion
            if isinstance(collection_names, str):
                return collection_names.split()
            return list(collection_names or [])

        # If the user provided a single collection name as a string, split it into a list.
        # Note: `list.split()` with no args splits on whitespace (and treats consecutive whitespace
        #       character as a single delimiter).
        if isinstance(collection_names, str):
            normalized_collection_names = collection_names.split()
        else:
            normalized_collection_names = list(collection_names)

        # Check for no collections.
        if len(normalized_collection_names) == 0:
            raise typer.BadParameter("At least one collection name must be specified.")

        # Check for empty strings or all-whitespace strings.
        for name in normalized_collection_names:
            if name == "":
                raise typer.BadParameter("Collection name cannot be an empty string.")
            elif name.strip() == "":
                raise typer.BadParameter("Collection name cannot consist of only whitespace.")

        # Check for duplicates and raise an error if there are any.
        distinct_collection_names = set()
        for name in normalized_collection_names:
            if name in distinct_collection_names:
                raise typer.BadParameter(f"Collection name '{name}' is specified more than once.")
            distinct_collection_names.add(name)

        return normalized_collection_names


# Note: We use `frozen=True` to prevent editing after initial instantiation.
@dataclass(frozen=True)
class DatabaseConfig:
    """Configuration for connecting to a MongoDB database."""

    host: str
    port: int
    username: str
    password: str
    name: str
    direct_connection: bool

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
        if self.direct_connection:
            kwargs.update({"directConnection": True})
        if self.is_auth_enabled:
            kwargs.update({"username": self.username, "password": self.password})
        return kwargs

    @contextmanager
    def make_temporary_config_file_path_context(self) -> Iterator[Path]:
        """
        Context manager that creates a temporary MongoDB tool config file containing the MongoDB
        password, and then yields the path to that file.

        That path can then be included (with the `--config` CLI option) in `mongodump` and `mongorestore`
        commands, instead of having to specify the password directly (via the `--password` CLI option).

        References:
        - https://docs.python.org/3/library/tempfile.html#tempfile.NamedTemporaryFile
        - https://www.mongodb.com/docs/database-tools/mongodump/#std-option-mongodump.--config
        - https://www.mongodb.com/docs/database-tools/mongorestore/#std-option-mongorestore.--config
        """

        with tempfile.NamedTemporaryFile(
            mode="w",
            prefix="mongo-tool-config",
            suffix=".yml",
            encoding="utf-8",
        ) as temporary_file:
            os.chmod(temporary_file.name, 0o600)
            json_escaped_password: str = json.dumps(self.password)
            temporary_file.write(f"password: {json_escaped_password}\n")
            temporary_file.seek(0)  # reset to beginning of file before yielding to consumers

            # Yield the path to the temporary config file.
            yield Path(temporary_file.name)

    def get_cli_options(
        self,
        include_db_option: bool = True,
        mongo_tool_config_file_path: Path | None = None,
    ) -> list[str]:
        """
        Get CLI options for connecting to a MongoDB database with this config.

        No authentication:
        >>> DatabaseConfig(
        ...     host="localhost",
        ...     port=27017,
        ...     username="",
        ...     password="",
        ...     name="my_database",
        ...     direct_connection=True,
        ... ).get_cli_options()
        ['--host', 'localhost', '--port', '27017', '--db', 'my_database']

        With authentication (user specified a config file path):
        >>> DatabaseConfig(
        ...     host="localhost",
        ...     port=27017,
        ...     username="my_user",
        ...     password="my_password",
        ...     name="my_database",
        ...     direct_connection=True,
        ... ).get_cli_options(mongo_tool_config_file_path=Path("/tmp/mongo-tool-config.yml"))
        ['--host', 'localhost', '--port', '27017', '--db', 'my_database', '--username', 'my_user', '--config', '/tmp/mongo-tool-config.yml', '--authenticationDatabase', 'admin']

        With authentication (user did not specify a config file path):
        >>> DatabaseConfig(
        ...     host="localhost",
        ...     port=27017,
        ...     username="my_user",
        ...     password="my_password",
        ...     name="my_database",
        ...     direct_connection=True,
        ... ).get_cli_options()
        Traceback (most recent call last):
        ...
        ValueError: Auth-enabled MongoDB tool invocations require a config file path. This is a safety measure to prevent credential exposure via the shell.
        """

        options = ["--host", self.host, "--port", str(self.port)]
        if include_db_option:
            options.extend(["--db", self.name])
        if self.is_auth_enabled:
            if not isinstance(mongo_tool_config_file_path, Path):
                raise ValueError(
                    "Auth-enabled MongoDB tool invocations require a config file path. "
                    "This is a safety measure to prevent credential exposure via the shell."
                )
            options.extend(
                [
                    "--username",
                    self.username,
                    "--config",
                    str(mongo_tool_config_file_path),
                    "--authenticationDatabase",
                    "admin",
                ]
            )
        return options

    @contextmanager
    def make_cli_options_context(self, include_db_option: bool = True) -> Iterator[list[str]]:
        r"""
        Context Manager that yields a list of `mongodump` or `mongorestore` CLI options related to
        accessing the MongoDB database described by this config.

        If authentication is enabled in this config, a temporary MongoDB tool config file containing
        the MongoDB password will be created and referenced in the yielded CLI options.

        Example without authentication:
        >>> with DatabaseConfig(
        ...     host="localhost",
        ...     port=27017,
        ...     username="",
        ...     password="",
        ...     name="my_database",
        ...     direct_connection=True,
        ... ).make_cli_options_context() as cli_options:
        ...     cli_options
        ['--host', 'localhost', '--port', '27017', '--db', 'my_database']

        Example with authentication:
        >>> from stat import filemode  # so we can print a human-readable chmod value
        >>> auth_config = DatabaseConfig(
        ...     host="localhost",
        ...     port=27017,
        ...     username="my_user",
        ...     password='pa"ss:#word\\name',
        ...     name="my_database",
        ...     direct_connection=True,
        ... )
        >>> with auth_config.make_cli_options_context() as cli_options:
        ...     index_of_config_file_flag = cli_options.index("--config")
        ...     config_file_path_str = cli_options[index_of_config_file_flag + 1]
        ...     config_file_path = Path(config_file_path_str)
        ...
        ...     config_file_path.exists() is True
        ...     filemode(config_file_path.stat().st_mode) == '-rw-------'
        ...     config_file_path.read_text() == 'password: "pa\\"ss:#word\\\\name"\n'
        True
        True
        True
        >>> config_file_path.exists()  # confirm the file gets deleted after exiting the context
        False
        """

        if self.is_auth_enabled:
            # Make a context so we have a config file path to use when getting CLI options.
            with self.make_temporary_config_file_path_context() as mongo_tool_config_file_path:
                yield self.get_cli_options(
                    include_db_option=include_db_option,
                    mongo_tool_config_file_path=mongo_tool_config_file_path,
                )
        else:
            yield self.get_cli_options(include_db_option=include_db_option)


@dataclass(frozen=True)
class MigrationConfig:
    """Overall configuration of the CLI app."""

    mongodump_path: Path
    mongorestore_path: Path
    origin_mongo_database_config: DatabaseConfig
    transformer_mongo_database_config: DatabaseConfig
    migrator_git_tag: str
    migrator_module_name: str
    schema_repo_url: str
    collection_names: list[str]
    origin_dump_folder_path: Path
    initial_origin_dump_path: Path | None
    transformer_dump_folder_path: Path
    auto_empty_origin_dump_folder: bool
    auto_empty_transformer_dump_folder: bool
    auto_drop_transformer_database: bool
    show_diff: bool
    skip_origin_writes: bool
    log_file_path: Path | None

    def get_redacted_dict(self) -> dict:
        """Get a representation of the config in which sensitive values have been redacted."""
        config_dict = asdict(self)
        config_dict["origin_mongo_database_config"] = self.origin_mongo_database_config.get_redacted_dict()
        config_dict["transformer_mongo_database_config"] = self.transformer_mongo_database_config.get_redacted_dict()
        return config_dict
