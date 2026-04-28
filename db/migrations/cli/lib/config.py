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

    mongosh_path: Path
    mongodump_path: Path
    mongorestore_path: Path
    origin_mongo_database_config: DatabaseConfig
    transformer_mongo_database_config: DatabaseConfig
    migrator_git_tag: str
    migrator_module_name: str
    schema_repo_url: str
    origin_dump_folder_path: Path
    transformer_dump_folder_path: Path

    def get_redacted_dict(self) -> dict:
        """Get a representation of the config in which sensitive values have been redacted."""
        config_dict = asdict(self)
        config_dict["origin_mongo_db_config"] = self.origin_mongo_database_config.get_redacted_dict()
        config_dict["transformer_mongo_db_config"] = self.transformer_mongo_database_config.get_redacted_dict()
        return config_dict
