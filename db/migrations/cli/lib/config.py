from typing import Optional
from dataclasses import asdict, dataclass
from os import access, X_OK
from pathlib import Path

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
        click.exceptions.BadParameter: Collection name cannot be an empty string.
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
        if isinstance(collection_names, str):
            normalized_collection_names = collection_names.split(" ")
        else:
            normalized_collection_names = list(collection_names)

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

    def get_cli_options(self, include_db_option: bool = True) -> list[str]:
        """Get a list of CLI options for connecting to a MongoDB database with this config."""
        options = ["--host", self.host, "--port", str(self.port)]
        if include_db_option:
            options.extend(["--db", self.name])
        if self.is_auth_enabled:
            options.extend(
                ["--username", self.username, "--password", self.password, "--authenticationDatabase", "admin"]
            )
        return options


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
    transformer_dump_folder_path: Path
    auto_empty_origin_dump_folder: bool
    auto_empty_transformer_dump_folder: bool
    auto_drop_transformer_database: bool

    def get_redacted_dict(self) -> dict:
        """Get a representation of the config in which sensitive values have been redacted."""
        config_dict = asdict(self)
        config_dict["origin_mongo_db_config"] = self.origin_mongo_database_config.get_redacted_dict()
        config_dict["transformer_mongo_db_config"] = self.transformer_mongo_database_config.get_redacted_dict()
        return config_dict
