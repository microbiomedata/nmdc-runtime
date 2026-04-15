from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Annotated

import typer
from rich import print


app = typer.Typer()


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


@dataclass(frozen=True)
class MigrationConfig:
    """Configuration for migrating a MongoDB database from one NMDC schema version to another."""

    mongosh_path: Path
    mongodump_path: Path
    mongorestore_path: Path
    origin_db: DatabaseConfig
    transformer_db: DatabaseConfig
    origin_schema_tag: str
    destination_schema_tag: str

    def get_redacted_dict(self) -> dict:
        """Get a representation of the config in which sensitive values have been redacted."""
        config_dict = asdict(self)
        config_dict["origin_db"] = self.origin_db.get_redacted_dict()
        config_dict["transformer_db"] = self.transformer_db.get_redacted_dict()
        return config_dict


def main(
    origin_schema_tag: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_SCHEMA_TAG",
            help="Git tag of the nmdc-schema version to which the origin database conforms.",
        ),
    ],
    destination_schema_tag: Annotated[
        str,
        typer.Option(
            envvar="DESTINATION_SCHEMA_TAG",
            help="Git tag of the nmdc-schema version to which you want to migrate the database.",
        ),
    ],
    origin_db_host: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_DB_HOST",
            help="Hostname for the origin MongoDB database.",
        ),
    ],
    origin_db_port: Annotated[
        int,
        typer.Option(
            envvar="ORIGIN_DB_PORT",
            help="Port number for the origin MongoDB database.",
        ),
    ] = 27017,
    origin_db_username: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_DB_USERNAME",
            help="Username for the origin MongoDB database. Leave empty for no auth.",
        ),
    ] = "",
    origin_db_password: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_DB_PASSWORD",
            help="Password for the origin MongoDB database.",
        ),
    ] = "",
    origin_db_name: Annotated[
        str,
        typer.Option(
            envvar="ORIGIN_DB_NAME",
            help="Database name for the origin MongoDB database.",
        ),
    ] = "nmdc",
    transformer_db_host: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_DB_HOST",
            help="Hostname for the transformer MongoDB database.",
        ),
    ] = "localhost",
    transformer_db_port: Annotated[
        int,
        typer.Option(
            envvar="TRANSFORMER_DB_PORT",
            help="Port number for the transformer MongoDB database.",
        ),
    ] = 27017,
    transformer_db_username: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_DB_USERNAME",
            help="Username for the transformer MongoDB database. Leave empty for no auth.",
        ),
    ] = "",
    transformer_db_password: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_DB_PASSWORD",
            help="Password for the transformer MongoDB database.",
        ),
    ] = "",
    transformer_db_name: Annotated[
        str,
        typer.Option(
            envvar="TRANSFORMER_DB_NAME",
            help="Database name for the transformer MongoDB database.",
        ),
    ] = "transformer",
    mongosh_path: Annotated[
        Path,
        typer.Option(
            dir_okay=False,
            exists=True,
            resolve_path=True,
            envvar="MONGOSH_PATH",
            help="Path to the `mongosh` executable.",
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
        ),
    ] = Path("/usr/bin/mongorestore"),
) -> None:
    """
    Migrate the NMDC database between two versions of the NMDC schema.
    """

    config = MigrationConfig(
        mongosh_path=mongosh_path,
        mongodump_path=mongodump_path,
        mongorestore_path=mongorestore_path,
        origin_db=DatabaseConfig(
            host=origin_db_host,
            port=origin_db_port,
            username=origin_db_username,
            password=origin_db_password,
            name=origin_db_name,
        ),
        transformer_db=DatabaseConfig(
            host=transformer_db_host,
            port=transformer_db_port,
            username=transformer_db_username,
            password=transformer_db_password,
            name=transformer_db_name,
        ),
        origin_schema_tag=origin_schema_tag,
        destination_schema_tag=destination_schema_tag,
    )

    print(config.get_redacted_dict())


def run() -> None:
    typer.run(main)


if __name__ == "__main__":
    run()
