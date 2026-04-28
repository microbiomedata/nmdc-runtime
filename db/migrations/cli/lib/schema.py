from inspect import isclass
from importlib import import_module
from typing import Type

import typer
from linkml.validator import Validator, SchemaDefinition
from linkml.validator.plugins import JsonschemaValidationPlugin
from linkml_runtime import SchemaView
from rich import print


def get_migrator_class(migrator_module_name: str) -> Type:
    """
    Imports the `Migrator` class from the specified module within the `nmdc_schema` package and returns a reference to it.
    """

    migrator_module = import_module(f".{migrator_module_name}", package="nmdc_schema.migrators")
    Migrator = getattr(migrator_module, "Migrator")  # gets the class
    if not isclass(Migrator):
        raise RuntimeError(f"Failed to import 'Migrator' class from module {migrator_module.__name__}")
    return Migrator

def get_mongo_adapter_class() -> Type:
    """
    Imports the `MongoAdapter` class from the `nmdc_schema` package and returns a reference to it.
    """

    mongo_adapter_module = import_module(".mongo_adapter", package="nmdc_schema.migrators.adapters")
    MongoAdapter = getattr(mongo_adapter_module, "MongoAdapter")  # gets the class
    if not isclass(MongoAdapter):
        raise RuntimeError(f"Failed to import 'MongoAdapter' class from module {mongo_adapter_module.__name__}")
    return MongoAdapter

def create_schema_definition() -> SchemaDefinition:
    """
    Returns a `SchemaDefinition` instance reflecting the NMDC schema.
    """

    # Import a function that returns a `SchemaDefinition` reflecting the NMDC schema.
    data_module = import_module(".nmdc_data", package="nmdc_schema")
    get_nmdc_schema_definition = getattr(data_module, "get_nmdc_schema_definition")  # gets the function
    if not callable(get_nmdc_schema_definition):
        raise RuntimeError(
            f"Failed to import 'get_nmdc_schema_definition' function from module {data_module.__name__}"
        )
    # Get a `SchemaDefinition` from the imported function.
    schema_definition = get_nmdc_schema_definition()
    if not isinstance(schema_definition, SchemaDefinition):
        raise RuntimeError(
            f"Failed to get NMDC schema definition from module {data_module.__name__}"
        )
    return schema_definition

def create_schema_view() -> SchemaView:
    """
    Returns a LinkML SchemaView instance that can be used to programmatically traverse the NMDC schema.
    """

    schema_definition = create_schema_definition()
    schema_view = SchemaView(schema_definition)
    return schema_view

def create_validator() -> Validator:
    """
    Creates and returns a `Validator` instance that can be used to validate data against the NMDC schema.

    References:
    - https://linkml.io/linkml/code/validator.html#linkml.validator.Validator
    - https://linkml.io/linkml/data/validating-data.html#validation-in-python-code
    """

    # Import the NMDC schema validation plugin, which we'll plug into to the validator.
    validation_plugin_module = import_module(".nmdc_schema_validation_plugin", package="nmdc_schema")
    NmdcSchemaValidationPlugin = getattr(validation_plugin_module, "NmdcSchemaValidationPlugin")  # gets the class
    if not isclass(NmdcSchemaValidationPlugin):
        raise typer.BadParameter(
            f"Failed to import 'NmdcSchemaValidationPlugin' class from module {validation_plugin_module.__name__}"
        )

    # Intantiate the validator.
    schema = create_schema_definition()
    validator = Validator(
        schema=schema,
        validation_plugins=[
            JsonschemaValidationPlugin(closed=True),
            NmdcSchemaValidationPlugin(),
        ]
    )

    print(f"Creating validator for NMDC schema version {schema.version}")

    return validator
