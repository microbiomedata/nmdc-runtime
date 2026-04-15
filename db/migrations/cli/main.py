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
    origin_mongo_database_config: DatabaseConfig
    transformer_mongo_database_config: DatabaseConfig
    origin_schema_tag: str
    destination_schema_tag: str

    def get_redacted_dict(self) -> dict:
        """Get a representation of the config in which sensitive values have been redacted."""
        config_dict = asdict(self)
        config_dict["origin_mongo_db_config"] = (
            self.origin_mongo_database_config.get_redacted_dict()
        )
        config_dict["transformer_mongo_db_config"] = (
            self.transformer_mongo_database_config.get_redacted_dict()
        )
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
            help="Git tag of the nmdc-schema version to which you want to migrate the origin database.",
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

    The origin database is the database you want to migrate. This app will dump data from the origin
    database, load it into the transformer database, transform it there so that it conforms to the
    destination schema, validate it there, dump the transformed data from the transformer database,
    and load it into the origin database (overwriting the original data there).

    This app does not support migrators that involve renaming MongoDB collection.
    """

    config = MigrationConfig(
        mongosh_path=mongosh_path,
        mongodump_path=mongodump_path,
        mongorestore_path=mongorestore_path,
        origin_mongo_database_config=DatabaseConfig(
            host=origin_mongo_host,
            port=origin_mongo_port,
            username=origin_mongo_username,
            password=origin_mongo_password,
            name=origin_mongo_database_name,
        ),
        transformer_mongo_database_config=DatabaseConfig(
            host=transformer_mongo_host,
            port=transformer_mongo_port,
            username=transformer_mongo_username,
            password=transformer_mongo_password,
            name=transformer_mongo_database_name,
        ),
        origin_schema_tag=origin_schema_tag,
        destination_schema_tag=destination_schema_tag,
    )

    print(config.get_redacted_dict())


def run() -> None:
    typer.run(main)


if __name__ == "__main__":
    run()
